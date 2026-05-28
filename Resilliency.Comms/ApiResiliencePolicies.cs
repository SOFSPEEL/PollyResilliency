using System.Net;
using Polly;
using Polly.CircuitBreaker;
using Polly.Fallback;
using Polly.Retry;
using Polly.Timeout;

namespace Resilliency.Comms;

public static class ApiResiliencePolicies
{
    public const string ServerUnavailableFallbackMessage =
        "We apoligize, we are working bring up our system ASAP";

    public static ResiliencePipeline<HttpResponseMessage> Create(
        Action<string> log,
        Action<CircuitBreakerVisualState>? circuitStateChanged = null,
        ApiResiliencePolicyOptions? options = null)
    {
        options ??= new ApiResiliencePolicyOptions();

        return new ResiliencePipelineBuilder<HttpResponseMessage>()
            .AddFallback(CreateFallbackForUnavailableServer(log))
            .AddRetry(CreateExponentialBackoffRetryFor529(log, options))
            .AddCircuitBreaker(CreateCircuitBreakerForRepeatedFailures(log, circuitStateChanged, options))
            .AddTimeout(CreatePerTryTimeout(log, options))
            .Build();
    }

    private static FallbackStrategyOptions<HttpResponseMessage> CreateFallbackForUnavailableServer(Action<string> log) =>
        new()
        {
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .Handle<HttpRequestException>(),
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
        ApiResiliencePolicyOptions options) =>
        new()
        {
            MaxRetryAttempts = options.MaxRetryAttempts,
            Delay = options.RetryDelay,
            BackoffType = DelayBackoffType.Exponential,
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .Handle<TimeoutRejectedException>()
                .HandleResult(response => (int)response.StatusCode == 529),
            OnRetry = args =>
            {
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
            FailureRatio = 1.0,
            MinimumThroughput = 4,
            SamplingDuration = TimeSpan.FromSeconds(10),
            BreakDuration = options.BreakDuration,
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .Handle<TimeoutRejectedException>()
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

    private static TimeoutStrategyOptions CreatePerTryTimeout(
        Action<string> log,
        ApiResiliencePolicyOptions options) =>
        new()
        {
            Timeout = options.Timeout,
            OnTimeout = args =>
            {
                log($"TIMEOUT: call exceeded {args.Timeout.TotalMilliseconds:N0} ms.");
                return default;
            }
        };

    private static string Describe(Outcome<HttpResponseMessage> outcome)
    {
        if (outcome.Exception is not null)
        {
            return outcome.Exception.GetType().Name;
        }

        return $"HTTP {(int)outcome.Result!.StatusCode} {outcome.Result.StatusCode}";
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
