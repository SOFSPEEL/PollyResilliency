using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Resilliency.Comms;
using Resilliency.Demo;

var port = GetAvailablePort();
var baseAddress = new Uri($"http://localhost:{port}");

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(baseAddress.ToString());

var events = new UiStateHub();
events.SetCircuitState(CircuitState.Closed);
var resilienceEventPublisher = EventHubResilienceEventPublisher.CreateInMemory();
var demoPolicyOptions = new ApiResiliencePolicyOptions
{
    CircuitFailureRatio = 0.75,
    MaxRetryAttempts = 3,
    CircuitMinimumThroughput = 5,
    SamplingDuration = TimeSpan.FromMinutes(2),
    RetryDelay = TimeSpan.FromSeconds(1),
    BreakDuration = TimeSpan.FromSeconds(8)
};

builder.Services.AddSingleton(events);
builder.Services.AddSingleton<IResilienceEventPublisher>(resilienceEventPublisher);
builder.Services.AddSingleton(new DemoScenarioOptions(demoPolicyOptions));
builder.Services.AddSingleton<DemoScenarioRunner>();
builder.Services.AddStatusCodeApiComms(
    baseAddress,
    message =>
    {
        var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | {message}";
        Console.WriteLine(line);
        var pollyLog = MapPollyLogEntry(message);
        if (pollyLog is not null)
        {
            events.AddLog(pollyLog);
        }

        if (IsFallbackMessage(message))
        {
            events.SetFallback(ApiResiliencePolicies.ServerUnavailableFallbackMessage, (int)HttpStatusCode.NotFound);
        }
    },
    state =>
    {
        var mappedState = MapCircuitState(state);
        events.SetCircuitState(mappedState);
        events.AddLog($"POLLY: CIRCUIT {FormatCircuitState(mappedState)}");
    },
    ServiceLifetime.Transient,
    demoPolicyOptions,
    retryBackoffObserved: backoff =>
    {
        events.MarkNextCallAsRetry(backoff.AttemptNumber);
        events.AddRetryBackoff(backoff.StatusCode, (int)Math.Round(backoff.Delay.TotalMilliseconds));
        events.AddLog($"POLLY: RETRY {backoff.AttemptNumber} ({backoff.Delay.TotalSeconds:0.#}s)");
    },
    resilienceEventPublisher);

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.MapDemoRoutes();

app.Lifetime.ApplicationStarted.Register(() =>
{
    Console.WriteLine($"Demo dashboard: {baseAddress}");
    TryOpenBrowser(baseAddress);
});

await app.RunAsync();

static int GetAvailablePort()
{
    var listener = new TcpListener(IPAddress.Loopback, port: 0);
    listener.Start();

    try
    {
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
    finally
    {
        listener.Stop();
    }
}

static void TryOpenBrowser(Uri uri)
{
    try
    {
        Process.Start(new ProcessStartInfo
        {
            FileName = uri.ToString(),
            UseShellExecute = true
        });
    }
    catch
    {
        Console.WriteLine($"Open {uri} in a browser to view the demo.");
    }
}

static CircuitState MapCircuitState(CircuitBreakerVisualState state) =>
    state switch
    {
        CircuitBreakerVisualState.Closed => CircuitState.Closed,
        CircuitBreakerVisualState.Open => CircuitState.Open,
        CircuitBreakerVisualState.HalfOpen => CircuitState.HalfOpen,
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, null)
    };

static string? MapPollyLogEntry(string message)
{
    if (message.StartsWith("FALLBACK: server unavailable", StringComparison.Ordinal))
    {
        return "POLLY: FALLBACK";
    }

    return null;
}

static bool IsFallbackMessage(string message) =>
    string.Equals(
        message,
        $"FALLBACK: {ApiResiliencePolicies.ServerUnavailableFallbackMessage}",
        StringComparison.Ordinal);

static string FormatCircuitState(CircuitState state) =>
    state switch
    {
        CircuitState.Closed => "CLOSED",
        CircuitState.Open => "OPEN",
        CircuitState.HalfOpen => "HALF-OPEN",
        _ => state.ToString().ToUpperInvariant()
    };
