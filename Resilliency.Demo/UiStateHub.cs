using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Resilliency.Demo;

internal sealed class UiStateHub
{
    private const int MaxEvents = 400;

    private readonly ConcurrentDictionary<Guid, Channel<UiState>> _subscribers = new();
    private readonly Lock _stateSync = new();
    private CircuitState _circuitState = CircuitState.Closed;
    private bool _isRunning;
    private int? _apiStatusCode;
    private string? _fallbackMessage;
    private readonly List<UiEvent> _events = [];
    private int _callCount;
    private int _lastRetryCallNumber;
    private string? _nextGraphLabel;

    public UiStateSubscription Subscribe()
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<UiState>();
        _subscribers[id] = channel;
        return new UiStateSubscription(channel.Reader, () => _subscribers.TryRemove(id, out _));
    }

    public void ResetScenario()
    {
        lock (_stateSync)
        {
            _apiStatusCode = null;
            _fallbackMessage = null;
            _events.Clear();
            _callCount = 0;
            _lastRetryCallNumber = 0;
            _nextGraphLabel = null;
            PublishUiStateChanged();
        }
    }

    public void ClearVisibleHistory()
    {
        lock (_stateSync)
        {
            _apiStatusCode = null;
            _fallbackMessage = null;
            _events.Clear();
            _callCount = 0;
            _lastRetryCallNumber = 0;
            _nextGraphLabel = null;
            PublishUiStateChanged();
        }
    }

    public void SetRunning(bool isRunning)
    {
        lock (_stateSync)
        {
            if (_isRunning == isRunning)
            {
                return;
            }

            _isRunning = isRunning;
            PublishUiStateChanged();
        }
    }

    public void AddLog(string message)
    {
        lock (_stateSync)
        {
            AddEvent(new UiEvent(Type: "log", Timestamp: CreateTimestamp(), Message: message));

            PublishUiStateChanged();
        }
    }

    public void SetCircuitState(CircuitState state)
    {
        lock (_stateSync)
        {
            if (_circuitState == state)
            {
                return;
            }

            _circuitState = state;
            if (state is CircuitState.HalfOpen)
            {
                _nextGraphLabel = FormatCircuitLabel(state);
            }

            PublishUiStateChanged();
        }
    }

    public void SetApiStatus(int statusCode)
    {
        lock (_stateSync)
        {
            _apiStatusCode = statusCode;
            PublishUiStateChanged();
        }
    }

    public void AddGraphBar(int statusCode)
    {
        lock (_stateSync)
        {
            _callCount += 1;
            var label = ResolveGraphLabel(statusCode);
            AddEvent(new UiEvent(
                Type: "graph",
                Timestamp: CreateTimestamp(),
                CallNumber: _callCount,
                StatusCode: statusCode,
                Label: label));
            if (statusCode == 529)
            {
                _lastRetryCallNumber = _callCount;
            }

            PublishUiStateChanged();
        }
    }

    public void AddRetryBackoff(int statusCode, int delayMs)
    {
        lock (_stateSync)
        {
            if (_lastRetryCallNumber <= 0)
            {
                return;
            }

            AddEvent(new UiEvent(
                Type: "backoff",
                Timestamp: CreateTimestamp(),
                CallNumber: _lastRetryCallNumber,
                StatusCode: statusCode,
                DelayMs: delayMs));
            PublishUiStateChanged();
        }
    }

    public void MarkNextCallAsRetry(int attemptNumber)
    {
        lock (_stateSync)
        {
            _nextGraphLabel = $"Retry {attemptNumber}";
            PublishUiStateChanged();
        }
    }

    public void SetFallback(string message, int statusCode)
    {
        lock (_stateSync)
        {
            _fallbackMessage = message;
            _apiStatusCode = statusCode;
            PublishUiStateChanged();
        }
    }

    public UiState CreateUiStateSnapshot()
    {
        lock (_stateSync)
        {
            return CreateUiState();
        }
    }

    private void Publish(UiState uiState)
    {
        foreach (var subscriber in _subscribers.Values)
        {
            subscriber.Writer.TryWrite(uiState);
        }
    }

    private void PublishUiStateChanged()
    {
        Publish(CreateUiState());
    }

    private void AddEvent(UiEvent uiEvent)
    {
        _events.Add(uiEvent);
        if (_events.Count > MaxEvents)
        {
            _events.RemoveAt(0);
        }
    }

    private static string CreateTimestamp() => DateTimeOffset.Now.ToString("HH:mm:ss.fff");

    private string ResolveGraphLabel(int statusCode)
    {
        if (!string.IsNullOrWhiteSpace(_nextGraphLabel))
        {
            var nextLabel = _nextGraphLabel;
            _nextGraphLabel = null;
            return nextLabel;
        }

        if (statusCode == 200)
        {
            return "Healthy";
        }

        if (statusCode == 529)
        {
            return FormatCircuitLabel(_circuitState);
        }

        return FormatCircuitLabel(_circuitState);
    }

    private static string FormatCircuitLabel(CircuitState state) =>
        state switch
        {
            CircuitState.Closed => "Closed",
            CircuitState.Open => "Open",
            CircuitState.HalfOpen => "Half-Open",
            _ => state.ToString()
        };

    private UiState CreateUiState() =>
        new(
            CircuitState: _circuitState,
            IsRunning: _isRunning,
            SendingHttp: _circuitState is CircuitState.Closed or CircuitState.HalfOpen,
            ApiStatusCode: _apiStatusCode,
            FallbackMessage: _fallbackMessage,
            Events: _events.ToArray());
}
