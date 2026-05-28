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
    private static readonly RespondingServerPlanStep[] RespondingServerPlan =
    [
        new(HttpStatusCode.OK, 6),
        new((HttpStatusCode)529, 0),
        new(HttpStatusCode.OK, 5),
        new(HttpStatusCode.NotFound, 5)
    ];

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
        int nextPlanIndex = 0;

        events.ResetScenario();
        events.SetCircuitState(CircuitState.Closed);
        await PauseForVisualStateAsync(pause, cancellationToken);

        await RunPlannedCallAsync(api, pipeline, GetNextPlannedStep(ref nextPlanIndex), cancellationToken);

        await RunPlannedCallAsync(api, pipeline, GetNextPlannedStep(ref nextPlanIndex), cancellationToken);

        var blockedByOpenCircuit = false;
        try
        {
            await pipeline.ExecuteAsync(
                token =>
                {
                    var plannedStep = GetNextPlannedStep(ref nextPlanIndex);
                    return new ValueTask<HttpResponseMessage>(
                        api.GetStatusAsync((int)plannedStep.StatusCode, delayMs: plannedStep.HoldSeconds * 1000, token));
                },
                cancellationToken);
        }
        catch (BrokenCircuitException)
        {
            blockedByOpenCircuit = true;
        }

        if (blockedByOpenCircuit)
        {
            await PauseForVisualStateAsync(demoOptions.PolicyOptions.BreakDuration + TimeSpan.FromMilliseconds(250), cancellationToken);
            await pipeline.ExecuteAsync(
                token =>
                {
                    var plannedStep = GetNextPlannedStep(ref nextPlanIndex);
                    return new ValueTask<HttpResponseMessage>(
                        api.GetStatusAsync((int)plannedStep.StatusCode, delayMs: plannedStep.HoldSeconds * 1000, token));
                },
                cancellationToken);
        }

    }

    private async Task RunPlannedCallAsync(
        IStatusCodeApi api,
        ResiliencePipeline<HttpResponseMessage> pipeline,
        RespondingServerPlanStep plannedStep,
        CancellationToken cancellationToken)
    {
        await pipeline.ExecuteAsync(
            token => new ValueTask<HttpResponseMessage>(
                api.GetStatusAsync((int)plannedStep.StatusCode, delayMs: plannedStep.HoldSeconds * 1000, token)),
            cancellationToken);
    }

    private static RespondingServerPlanStep GetNextPlannedStep(ref int nextPlanIndex)
    {
        if (nextPlanIndex >= RespondingServerPlan.Length)
        {
            return new RespondingServerPlanStep(HttpStatusCode.OK, 0);
        }

        var plannedStatus = RespondingServerPlan[nextPlanIndex];
        nextPlanIndex++;
        return plannedStatus;
    }

    private readonly record struct RespondingServerPlanStep(HttpStatusCode StatusCode, int HoldSeconds);

    private static async Task PauseForVisualStateAsync(TimeSpan duration, CancellationToken cancellationToken)
    {
        await Task.Delay(duration, cancellationToken);
    }
}