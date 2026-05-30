namespace Resilliency.Demo;

internal sealed record UiState(
    CircuitState CircuitState,
    bool IsRunning,
    bool SendingHttp,
    int? ApiStatusCode,
    string? FallbackMessage,
    IReadOnlyList<UiGraphEntry> GraphEntries,
    IReadOnlyList<UiBackoffEntry> BackoffEntries,
    IReadOnlyList<UiLogEntry> LogEntries);

internal sealed record UiGraphEntry(int CallNumber, int StatusCode, string Label);

internal sealed record UiBackoffEntry(int CallNumber, int StatusCode, int DelayMs);

internal sealed record UiLogEntry(string Timestamp, string Message);
