using Polly;
using Polly.CircuitBreaker;
using Polly.Timeout;
using Refit;
using Resilliency.Comms;

namespace Resilliency.Demo;

internal sealed class DemoScenarioRunner(
    IServiceProvider services,
    DemoEventHub events,
    DemoScenarioOptions options)
{
    private readonly SemaphoreSlim _scenarioLock = new(1, 1);
    private CancellationTokenSource? _currentRunCancellation;

    public async Task<bool> TryRunAsync(CancellationToken cancellationToken)
    {
        return await StartAsync(restart: false, cancellationToken);
    }

    public async Task RestartAsync(CancellationToken cancellationToken)
    {
        await StartAsync(restart: true, cancellationToken);
    }

    private async Task<bool> StartAsync(bool restart, CancellationToken cancellationToken)
    {
        if (!await _scenarioLock.WaitAsync(0, cancellationToken))
        {
            if (!restart)
            {
                return false;
            }

            _currentRunCancellation?.Cancel();
            await _scenarioLock.WaitAsync(cancellationToken);
        }

        var runCancellation = new CancellationTokenSource();
        _currentRunCancellation = runCancellation;

        _ = Task.Run(async () =>
        {
            try
            {
                await RunAsync(runCancellation.Token);
            }
            catch (OperationCanceledException) when (runCancellation.IsCancellationRequested)
            {
                var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | DEMO RESTART: previous run was cancelled.";
                Console.WriteLine(line);
                events.Publish("log", line);
            }
            catch (Exception ex)
            {
                var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | DEMO ERROR: {ex}";
                Console.WriteLine(line);
                events.Publish("log", line);
            }
            finally
            {
                if (ReferenceEquals(_currentRunCancellation, runCancellation))
                {
                    _currentRunCancellation = null;
                }

                runCancellation.Dispose();
                _scenarioLock.Release();
            }
        }, CancellationToken.None);

        return true;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        var pause = TimeSpan.FromSeconds(5);
        var api = services.GetRequiredService<IStatusCodeApi>();
        var pipeline = services.GetRequiredService<ResiliencePipeline<HttpResponseMessage>>();

        events.Publish("state", "Circuit starts closed", CircuitBreakerVisualState.Closed.ToString());
        await PauseForVisualStateAsync("PAUSE: closed circuit, calls can flow to the API.", pause, cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 0: healthy HTTP 200 call number 1 flows through the closed circuit.");
        await RunHealthyCallAsync(api, pipeline, 1, cancellationToken);
        await PauseForVisualStateAsync("PAUSE: first healthy call completed successfully.", TimeSpan.FromSeconds(3), cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 0: healthy HTTP 200 call number 2 also flows through the closed circuit.");
        await RunHealthyCallAsync(api, pipeline, 2, cancellationToken);
        await PauseForVisualStateAsync("PAUSE: second healthy call completed successfully.", TimeSpan.FromSeconds(3), cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 1: send HTTP 529; exponential backoff waits longer between retries so we are not hammering the server.");
        var failedResponse = await pipeline.ExecuteAsync(
            token => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(529, delayMs: 0, token)),
            cancellationToken);
        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | RESULT: final response was {(int)failedResponse.StatusCode} {failedResponse.StatusCode}.");
        await PauseForVisualStateAsync("PAUSE: open circuit, calls are blocked before HTTP.", TimeSpan.FromSeconds(5), cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 2: request HTTP 200 while open; Polly blocks before HTTP.");
        try
        {
            await pipeline.ExecuteAsync(
                token => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 0, token)),
                cancellationToken);
        }
        catch (BrokenCircuitException ex)
        {
            events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | RESULT: blocked by open circuit: {ex.GetType().Name}.");
        }

        await PauseForVisualStateAsync("PAUSE: still open, waiting for the breaker window to expire.", pause, cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 3: wait for half-open, then send HTTP 200 probe to close.");
        await Task.Delay(TimeSpan.FromSeconds(8.1), cancellationToken);
        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | PAUSE: next call will show half-open probe behavior.");
        await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

        var recoveredResponse = await pipeline.ExecuteAsync(
            token => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 0, token)),
            cancellationToken);
        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | RESULT: recovery response was {(int)recoveredResponse.StatusCode} {recoveredResponse.StatusCode}.");
        await PauseForVisualStateAsync("PAUSE: circuit is closed again after the successful probe.", TimeSpan.FromSeconds(5), cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 4: slow HTTP 200 exceeds timeout; retry attempts it again.");
        try
        {
            await pipeline.ExecuteAsync(
                token => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 2_000, token)),
                cancellationToken);
        }
        catch (TimeoutRejectedException ex)
        {
            events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | RESULT: timed out after retries: {ex.GetType().Name}.");
        }

        await PauseForVisualStateAsync("PAUSE: timeout failures opened the circuit again.", TimeSpan.FromSeconds(5), cancellationToken);

        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | SCENARIO 5: call a server-down URL; fallback returns the customer apology message.");
        var downApi = RestService.For<IStatusCodeApi>("http://127.0.0.1:1");
        var downPipeline = ApiResiliencePolicies.Create(message =>
        {
            var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | {message}";
            Console.WriteLine(line);
            events.Publish("log", line);
        }, options: options.PolicyOptions);
        var fallbackResponse = await downPipeline.ExecuteAsync(
            token => new ValueTask<HttpResponseMessage>(downApi.GetStatusAsync(200, delayMs: 0, token)),
            cancellationToken);
        var fallbackMessage = await fallbackResponse.Content.ReadAsStringAsync(cancellationToken);
        events.Publish("api-status", $"HTTP {(int)fallbackResponse.StatusCode}", ((int)fallbackResponse.StatusCode).ToString());
        events.Publish("fallback", fallbackMessage, ((int)fallbackResponse.StatusCode).ToString());
        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | FALLBACK RESPONSE: {fallbackMessage}");
        await PauseForVisualStateAsync("PAUSE: fallback is the final state.", TimeSpan.FromSeconds(6), cancellationToken);
    }

    private async Task RunHealthyCallAsync(
        IStatusCodeApi api,
        ResiliencePipeline<HttpResponseMessage> pipeline,
        int callNumber,
        CancellationToken cancellationToken)
    {
        var response = await pipeline.ExecuteAsync(
            token => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 0, token)),
            cancellationToken);
        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | RESULT: healthy call {callNumber} returned {(int)response.StatusCode} {response.StatusCode}.");
    }

    private async Task PauseForVisualStateAsync(
        string message,
        TimeSpan duration,
        CancellationToken cancellationToken)
    {
        events.Publish("log", $"{DateTimeOffset.Now:HH:mm:ss.fff} | {message}");
        await Task.Delay(duration, cancellationToken);
    }
}
