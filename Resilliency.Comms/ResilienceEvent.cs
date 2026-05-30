namespace Resilliency.Comms;

public sealed record ResilienceEvent(
    string EventType,
    string Message,
    DateTimeOffset Timestamp,
    int? StatusCode = null,
    int? AttemptNumber = null,
    int? DelayMilliseconds = null,
    string? CircuitState = null);
