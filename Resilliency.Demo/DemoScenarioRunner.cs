using Polly;
using Polly.CircuitBreaker;
using System.Net;
using Resilliency.Comms;

namespace Resilliency.Demo;

internal sealed class DemoScenarioRunner(
    IServiceProvider services,
    UiStateHub events,
    DemoScenarioOptions demoOptions)
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

            var currentRunCancellation = _currentRunCancellation;
            if (currentRunCancellation is not null)
            {
                await currentRunCancellation.CancelAsync();
            }
            await _scenarioLock.WaitAsync(cancellationToken);
        }

        var runCancellation = new CancellationTokenSource();
        _currentRunCancellation = runCancellation;
        events.SetRunning(true);

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
            }
            catch (Exception ex)
            {
                var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | DEMO ERROR: {ex}";
                Console.WriteLine(line);
            }
            finally
            {
                if (ReferenceEquals(_currentRunCancellation, runCancellation))
                {
                    _currentRunCancellation = null;
                }

                events.SetRunning(false);
                runCancellation.Dispose();
                _scenarioLock.Release();
            }
        }, CancellationToken.None);

        return true;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        var api = services.GetRequiredService<IStatusCodeApi>();
        var pipeline = services.GetRequiredService<ResiliencePipeline<HttpResponseMessage>>();

        events.ResetScenario();
        events.SetCircuitState(CircuitState.Closed);

        foreach (var plannedStep in CreateMockDownstreamServerPlan())
        {
            await RunPlannedStepAsync(api, pipeline, plannedStep, cancellationToken);
        }
    }

    private async Task RunPlannedStepAsync(
        IStatusCodeApi api,
        ResiliencePipeline<HttpResponseMessage> pipeline,
        MockDownstreamServerPlanStep plannedStep,
        CancellationToken cancellationToken)
    {
        if (plannedStep.WaitBeforeStep > TimeSpan.Zero)
        {
            await Task.Delay(plannedStep.WaitBeforeStep, cancellationToken);
        }

        if (plannedStep.StatusCode is null)
        {
            return;
        }

        try
        {
            await pipeline.ExecuteAsync(
                token => new ValueTask<HttpResponseMessage>(
                    api.GetStatusAsync((int)plannedStep.StatusCode.Value, delayMs: 0, token)),
                cancellationToken);
        }
        catch
        {
        }
    }

    private MockDownstreamServerPlanStep[] CreateMockDownstreamServerPlan() =>
    [
        new(StatusCode: null, WaitBeforeStep: TimeSpan.FromSeconds(10)),
        new(HttpStatusCode.OK),
        new((HttpStatusCode)529, WaitBeforeStep: TimeSpan.FromSeconds(12)),
        new(HttpStatusCode.OK, WaitBeforeStep: TimeSpan.FromSeconds(8)),
        new(
            HttpStatusCode.OK,
            WaitBeforeStep: demoOptions.PolicyOptions.BreakDuration + TimeSpan.FromSeconds(12)),
        new(HttpStatusCode.NotFound, WaitBeforeStep: TimeSpan.FromSeconds(11))
    ];

    private readonly record struct MockDownstreamServerPlanStep(
        HttpStatusCode? StatusCode,
        TimeSpan WaitBeforeStep = default);
}
