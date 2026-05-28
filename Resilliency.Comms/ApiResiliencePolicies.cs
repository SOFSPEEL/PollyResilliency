using System.Net;
using Polly;
using Polly.CircuitBreaker;
using Polly.Fallback;
using Polly.Retry;

namespace Resilliency.Comms;

public static class ApiResiliencePolicies
{
    public const string ServerUnavailableFallbackMessage =
        "We apoligize, we are working bring up our system ASAP";

    public static ResiliencePipeline<HttpResponseMessage> Create(
        Action<string> log,
        Action<CircuitBreakerVisualState>? circuitStateChanged = null,
        ApiResiliencePolicyOptions? options = null,
        Action<(int StatusCode, int AttemptNumber, TimeSpan Delay)>? retryBackoffObserved = null)
    {
        options ??= new ApiResiliencePolicyOptions();

        return new ResiliencePipelineBuilder<HttpResponseMessage>()
            .AddFallback(CreateFallbackForUnavailableServer(log))
            .AddRetry(CreateExponentialBackoffRetryFor529(log, options, retryBackoffObserved))
            .AddCircuitBreaker(CreateCircuitBreakerForRepeatedFailures(log, circuitStateChanged, options))
            .Build();
    }

    private static FallbackStrategyOptions<HttpResponseMessage> CreateFallbackForUnavailableServer(Action<string> log) =>
        new()
        {
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .Handle<HttpRequestException>()
                .HandleResult(response => response.StatusCode == HttpStatusCode.NotFound),
            FallbackAction = _ =>
            {
                log($"FALLBACK: {ServerUnavailableFallbackMessage}");
                var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent(ServerUnavailableFallbackMessage)
                };

                return Outcome.FromResultAsValueTask(response);
            },
            OnFallback = args =>
            {
                log($"FALLBACK: server unavailable after resilience handling: {Describe(args.Outcome)}.");
                return default;
            }
        };

    private static RetryStrategyOptions<HttpResponseMessage> CreateExponentialBackoffRetryFor529(
        Action<string> log,
        ApiResiliencePolicyOptions options,
        Action<(int StatusCode, int AttemptNumber, TimeSpan Delay)>? retryBackoffObserved) =>
        new()
        {
            MaxRetryAttempts = options.MaxRetryAttempts,
            Delay = options.RetryDelay,
            DelayGenerator = args => new ValueTask<TimeSpan?>(
                TimeSpan.FromMilliseconds(options.RetryDelay.TotalMilliseconds * Math.Pow(3, args.AttemptNumber))),
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .HandleResult(response => (int)response.StatusCode == 529),
            OnRetry = args =>
            {
                if (args.Outcome.Result is not null)
                {
                    retryBackoffObserved?.Invoke(((int)args.Outcome.Result.StatusCode, args.AttemptNumber + 1, args.RetryDelay));
                }

                log(
                    $"EXPONENTIAL BACKOFF: retry {args.AttemptNumber + 1} after {Describe(args.Outcome)}; waiting {args.RetryDelay.TotalMilliseconds:N0} ms so we are not hammering the server.");
                return default;
            }
        };

    private static CircuitBreakerStrategyOptions<HttpResponseMessage> CreateCircuitBreakerForRepeatedFailures(
        Action<string> log,
        Action<CircuitBreakerVisualState>? circuitStateChanged,
        ApiResiliencePolicyOptions options) =>
        new()
        {
            FailureRatio = options.CircuitFailureRatio,
            MinimumThroughput = options.CircuitMinimumThroughput,
            SamplingDuration = options.SamplingDuration,
            BreakDuration = options.BreakDuration,
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .HandleResult(response => (int)response.StatusCode == 529),
            OnOpened = args =>
            {
                log(
                    $"CIRCUIT OPEN: failures reached threshold; blocking calls for {args.BreakDuration.TotalSeconds:N0} seconds.");
                LogCircuitState(log, CircuitBreakerVisualState.Open);
                circuitStateChanged?.Invoke(CircuitBreakerVisualState.Open);
                return default;
            },
            OnHalfOpened = _ =>
            {
                log("CIRCUIT HALF-OPEN: next call is a probe.");
                LogCircuitState(log, CircuitBreakerVisualState.HalfOpen);
                circuitStateChanged?.Invoke(CircuitBreakerVisualState.HalfOpen);
                return default;
            },
            OnClosed = _ =>
            {
                log("CIRCUIT CLOSED: probe succeeded; normal traffic resumes.");
                LogCircuitState(log, CircuitBreakerVisualState.Closed);
                circuitStateChanged?.Invoke(CircuitBreakerVisualState.Closed);
                return default;
            }
        };

    private static string Describe(Outcome<HttpResponseMessage> outcome)
    {
        if (outcome.Exception is not null)
        {
            return outcome.Exception.GetType().Name;
        }

        if (outcome.Result is not null)
        {
            return $"HTTP {(int)outcome.Result.StatusCode} {outcome.Result.StatusCode}";
        }

        return "No response";
    }

    private static void LogCircuitState(Action<string> log, CircuitBreakerVisualState state)
    {
        log("CIRCUIT STATE:");
        log($"  client {Wire(state is CircuitBreakerVisualState.Closed or CircuitBreakerVisualState.HalfOpen)} api");
        log($"         {StateLabel(state)}");
        log("  CLOSED    -> calls flow normally");
        log("  OPEN      -> calls fail fast before HTTP");
        log("  HALF-OPEN -> one probe call decides recovery");
    }

    private static string Wire(bool connected) => connected ? "=========>" : "===X====>";

    private static string StateLabel(CircuitBreakerVisualState state) =>
        state switch
        {
            CircuitBreakerVisualState.Closed => "[ CLOSED ]",
            CircuitBreakerVisualState.Open => "[  OPEN  ]",
            CircuitBreakerVisualState.HalfOpen => "[HALF-OPEN]",
            _ => throw new ArgumentOutOfRangeException(nameof(state), state, null)
        };
}
