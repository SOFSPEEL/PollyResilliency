namespace Resilliency.Demo;

internal sealed record UiState(
    CircuitState CircuitState,
    bool IsRunning,
    bool SendingHttp,
    int? ApiStatusCode,
    string? FallbackMessage,
    IReadOnlyList<UiEvent> Events);

internal sealed record UiEvent(
    string Type,
    string Timestamp,
    string? Message = null,
    int? CallNumber = null,
    int? StatusCode = null,
    string? Label = null,
    int? DelayMs = null);
