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
        Action<(int StatusCode, int AttemptNumber, TimeSpan Delay)>? retryBackoffObserved = null,
        IResilienceEventPublisher? resilienceEventPublisher = null)
    {
        options ??= new ApiResiliencePolicyOptions();

        return new ResiliencePipelineBuilder<HttpResponseMessage>()
            .AddFallback(CreateFallbackForUnavailableServer(log, resilienceEventPublisher))
            .AddRetry(CreateExponentialBackoffRetryFor529(options, retryBackoffObserved, resilienceEventPublisher, log))
            .AddCircuitBreaker(CreateCircuitBreakerForRepeatedFailures(circuitStateChanged, options, resilienceEventPublisher, log))
            .Build();
    }

    private static FallbackStrategyOptions<HttpResponseMessage> CreateFallbackForUnavailableServer(
        Action<string> log,
        IResilienceEventPublisher? resilienceEventPublisher) =>
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
            OnFallback = async args =>
            {
                var message = $"FALLBACK: server unavailable after resilience handling: {Describe(args.Outcome)}.";
                log(message);
                await PublishSafelyAsync(
                    resilienceEventPublisher,
                    new ResilienceEvent(
                        EventType: "fallback",
                        Message: message,
                        Timestamp: DateTimeOffset.UtcNow,
                        StatusCode: args.Outcome.Result is null ? null : (int)args.Outcome.Result.StatusCode),
                    log,
                    args.Context.CancellationToken);
            }
        };

    private static RetryStrategyOptions<HttpResponseMessage> CreateExponentialBackoffRetryFor529(ApiResiliencePolicyOptions options,
        Action<(int StatusCode, int AttemptNumber, TimeSpan Delay)>? retryBackoffObserved,
        IResilienceEventPublisher? resilienceEventPublisher,
        Action<string> log) =>
        new()
        {
            MaxRetryAttempts = options.MaxRetryAttempts,
            Delay = options.RetryDelay,
            DelayGenerator = args => new ValueTask<TimeSpan?>(
                TimeSpan.FromMilliseconds(options.RetryDelay.TotalMilliseconds * Math.Pow(4, args.AttemptNumber))),
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .HandleResult(response => (int)response.StatusCode == 529),
            OnRetry = args =>
            {
                if (args.Outcome.Result is not null)
                {
                    var statusCode = (int)args.Outcome.Result.StatusCode;
                    var attemptNumber = args.AttemptNumber + 1;
                    retryBackoffObserved?.Invoke((statusCode, attemptNumber, args.RetryDelay));
                    return PublishSafelyAsync(
                        resilienceEventPublisher,
                        new ResilienceEvent(
                            EventType: "retry",
                            Message: $"Retry {attemptNumber} for HTTP {statusCode} after {args.RetryDelay.TotalMilliseconds:0} ms.",
                            Timestamp: DateTimeOffset.UtcNow,
                            StatusCode: statusCode,
                            AttemptNumber: attemptNumber,
                            DelayMilliseconds: (int)Math.Round(args.RetryDelay.TotalMilliseconds)),
                        log,
                        args.Context.CancellationToken);
                }

                return default;
            }
        };

    private static CircuitBreakerStrategyOptions<HttpResponseMessage> CreateCircuitBreakerForRepeatedFailures(Action<CircuitBreakerVisualState>? circuitStateChanged,
        ApiResiliencePolicyOptions options,
        IResilienceEventPublisher? resilienceEventPublisher,
        Action<string> log) =>
        new()
        {
            FailureRatio = options.CircuitFailureRatio,
            MinimumThroughput = options.CircuitMinimumThroughput,
            SamplingDuration = options.SamplingDuration,
            BreakDuration = options.BreakDuration,
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .HandleResult(response => (int)response.StatusCode == 529),
            OnOpened = _ =>
            {
                return OnCircuitStateChangedAsync(
                    CircuitBreakerVisualState.Open,
                    circuitStateChanged,
                    resilienceEventPublisher,
                    log,
                    CancellationToken.None);
            },
            OnHalfOpened = _ =>
            {
                return OnCircuitStateChangedAsync(
                    CircuitBreakerVisualState.HalfOpen,
                    circuitStateChanged,
                    resilienceEventPublisher,
                    log,
                    CancellationToken.None);
            },
            OnClosed = _ =>
            {
                return OnCircuitStateChangedAsync(
                    CircuitBreakerVisualState.Closed,
                    circuitStateChanged,
                    resilienceEventPublisher,
                    log,
                    CancellationToken.None);
            }
        };

    private static ValueTask OnCircuitStateChangedAsync(
        CircuitBreakerVisualState state,
        Action<CircuitBreakerVisualState>? circuitStateChanged,
        IResilienceEventPublisher? resilienceEventPublisher,
        Action<string> log,
        CancellationToken cancellationToken)
    {
        circuitStateChanged?.Invoke(state);
        return PublishSafelyAsync(
            resilienceEventPublisher,
            new ResilienceEvent(
                EventType: "circuit-state",
                Message: $"Circuit is {state}.",
                Timestamp: DateTimeOffset.UtcNow,
                CircuitState: state.ToString()),
            log,
            cancellationToken);
    }

    private static async ValueTask PublishSafelyAsync(
        IResilienceEventPublisher? resilienceEventPublisher,
        ResilienceEvent resilienceEvent,
        Action<string> log,
        CancellationToken cancellationToken)
    {
        if (resilienceEventPublisher is null)
        {
            return;
        }

        try
        {
            await resilienceEventPublisher.PublishAsync(resilienceEvent, cancellationToken);
        }
        catch (Exception ex)
        {
            log($"EVENTHUB: failed to publish {resilienceEvent.EventType} event: {ex.Message}");
        }
    }

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
}
