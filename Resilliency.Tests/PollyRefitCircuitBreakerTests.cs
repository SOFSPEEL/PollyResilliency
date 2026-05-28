using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.CircuitBreaker;
using Polly.Timeout;
using Resilliency.Comms;

namespace Resilliency.Tests;

public class PollyRefitCircuitBreakerTests
{
    [Test]
    public async Task Polly_pipeline_chains_timeout_circuit_breaker_and_retry_around_refit_call()
    {
        await using var server = await LocalStatusCodeServer.StartAsync();
        await using var serviceProvider = CreateServiceProvider(server.BaseAddress);
        var api = serviceProvider.GetRequiredService<IStatusCodeApi>();
        var pipeline = serviceProvider.GetRequiredService<ResiliencePipeline<HttpResponseMessage>>();

        Log($"Server listening at {server.BaseAddress}");
        Log("SCENARIO 1: call /status/529; exponential backoff retries avoid hammering the server, then the circuit opens.");
        var failedResponse = await pipeline.ExecuteAsync(
            cancellationToken => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(529, delayMs: 0, cancellationToken)),
            TestContext.CurrentContext.CancellationToken);

        Log($"RESULT: final response was {(int)failedResponse.StatusCode} {failedResponse.StatusCode}.");

        Log("SCENARIO 2: immediately call /status/200 while the circuit is open; Polly blocks the call before Refit reaches the server.");
        var blocked = Assert.ThrowsAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(
                cancellationToken => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 0, cancellationToken)),
                TestContext.CurrentContext.CancellationToken));

        Log($"RESULT: blocked by open circuit: {blocked!.GetType().Name}.");

        Log("SCENARIO 3: wait for break duration, then call /status/200; the half-open probe succeeds and closes the circuit.");
        await Task.Delay(TimeSpan.FromSeconds(2.1), TestContext.CurrentContext.CancellationToken);

        var recoveredResponse = await pipeline.ExecuteAsync(
            cancellationToken => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 0, cancellationToken)),
            TestContext.CurrentContext.CancellationToken);

        Log($"RESULT: recovery response was {(int)recoveredResponse.StatusCode} {recoveredResponse.StatusCode}.");

        Log("SCENARIO 4: call /status/200 with a server delay longer than the timeout; Polly times out and retries.");
        var timedOut = Assert.ThrowsAsync<TimeoutRejectedException>(async () =>
            await pipeline.ExecuteAsync(
                cancellationToken => new ValueTask<HttpResponseMessage>(api.GetStatusAsync(200, delayMs: 1_000, cancellationToken)),
                TestContext.CurrentContext.CancellationToken));

        Log($"RESULT: timed out after retries: {timedOut!.GetType().Name}.");

        Log("SCENARIO 5: call a server-down URL; fallback returns the customer apology message.");
        await using var serverDownProvider = CreateServiceProvider(new Uri($"http://127.0.0.1:{GetAvailablePort()}/"));
        var serverDownApi = serverDownProvider.GetRequiredService<IStatusCodeApi>();
        var serverDownPipeline = serverDownProvider.GetRequiredService<ResiliencePipeline<HttpResponseMessage>>();
        var fallbackResponse = await serverDownPipeline.ExecuteAsync(
            cancellationToken => new ValueTask<HttpResponseMessage>(serverDownApi.GetStatusAsync(200, delayMs: 0, cancellationToken)),
            TestContext.CurrentContext.CancellationToken);
        var fallbackMessage = await fallbackResponse.Content.ReadAsStringAsync(TestContext.CurrentContext.CancellationToken);

        Log($"RESULT: fallback response was {(int)fallbackResponse.StatusCode} {fallbackResponse.StatusCode}: {fallbackMessage}");

        Assert.Multiple(() =>
        {
            Assert.That((int)failedResponse.StatusCode, Is.EqualTo(529));
            Assert.That(recoveredResponse.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(fallbackResponse.StatusCode, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(fallbackMessage, Is.EqualTo(ApiResiliencePolicies.ServerUnavailableFallbackMessage));
        });
    }

    private static void Log(string message)
    {
        var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | {message}";
        Console.WriteLine(line);
    }

    private static ServiceProvider CreateServiceProvider(Uri baseAddress)
    {
        var services = new ServiceCollection();
        services.AddStatusCodeApiComms(baseAddress, Log);

        return services.BuildServiceProvider();
    }

    private static int GetAvailablePort()
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

    private sealed class LocalStatusCodeServer : IAsyncDisposable
    {
        private readonly CancellationTokenSource _shutdown = new();
        private readonly HttpListener _listener = new();
        private readonly Task _serverTask;

        private LocalStatusCodeServer(Uri baseAddress)
        {
            BaseAddress = baseAddress;
            _listener.Prefixes.Add(baseAddress.ToString());
            _listener.Start();
            _serverTask = Task.Run(RunAsync);
        }

        public Uri BaseAddress { get; }

        public static Task<LocalStatusCodeServer> StartAsync()
        {
            var port = GetAvailablePort();
            var baseAddress = new Uri($"http://127.0.0.1:{port}/");
            return Task.FromResult(new LocalStatusCodeServer(baseAddress));
        }

        public async ValueTask DisposeAsync()
        {
            _shutdown.Cancel();
            _listener.Stop();
            _listener.Close();

            try
            {
                await _serverTask;
            }
            catch (ObjectDisposedException)
            {
            }
            catch (HttpListenerException)
            {
            }

            _shutdown.Dispose();
        }

        private async Task RunAsync()
        {
            while (!_shutdown.IsCancellationRequested)
            {
                HttpListenerContext context;

                try
                {
                    context = await _listener.GetContextAsync().WaitAsync(_shutdown.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                _ = Task.Run(() => HandleAsync(context), _shutdown.Token);
            }
        }

        private async Task HandleAsync(HttpListenerContext context)
        {
            var request = context.Request;
            var response = context.Response;

            var statusCodeText = request.Url?.Segments.LastOrDefault();
            var statusCode = int.TryParse(statusCodeText, out var parsedStatusCode)
                ? parsedStatusCode
                : StatusCodes.BadRequest;

            var delayMs = int.TryParse(request.QueryString["delayMs"], out var parsedDelayMs)
                ? parsedDelayMs
                : 0;

            Log($"SERVER: received {request.HttpMethod} {request.RawUrl}; returning HTTP {statusCode} after {delayMs} ms.");

            if (delayMs > 0)
            {
                await Task.Delay(delayMs);
            }

            response.StatusCode = statusCode;
            response.ContentType = "text/plain";

            await using var output = response.OutputStream;
            await using var writer = new StreamWriter(output);
            await writer.WriteAsync($"HTTP {statusCode}");
        }

        private static int GetAvailablePort()
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
    }

    private static class StatusCodes
    {
        public const int BadRequest = 400;
    }
}
