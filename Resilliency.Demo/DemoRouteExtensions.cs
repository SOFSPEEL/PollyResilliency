using System.Text.Json;

namespace Resilliency.Demo;

internal static class DemoRouteExtensions
{
    public static void MapDemoRoutes(this WebApplication app)
    {
        app.MapGet("/events", StreamEventsAsync);
        app.MapPost("/run", RunScenarioAsync);
        app.MapPost("/restart", RestartScenarioAsync);
        app.MapGet("/status/{statusCode:int}", StatusCodeAsync);
    }

    private static async Task StreamEventsAsync(HttpContext context, UiStateHub eventHub)
    {
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";
        context.Response.ContentType = "text/event-stream";

        var subscription = eventHub.Subscribe();
        await WriteEventAsync(context, eventHub.CreateUiStateSnapshot());

        try
        {
            await foreach (var demoEvent in subscription.Reader.ReadAllAsync(context.RequestAborted))
            {
                await WriteEventAsync(context, demoEvent);
            }
        }
        finally
        {
            subscription.Dispose();
        }
    }

    private static async Task<IResult> RunScenarioAsync(
        DemoScenarioRunner runner,
        CancellationToken cancellationToken)
    {
        var started = await runner.TryRunAsync(cancellationToken);
        return Results.Accepted(value: new { status = started ? "started" : "already-running" });
    }

    private static async Task<IResult> RestartScenarioAsync(
        DemoScenarioRunner runner,
        UiStateHub eventHub,
        CancellationToken cancellationToken)
    {
        eventHub.ResetScenario();
        eventHub.SetCircuitState(CircuitState.Closed);
        await runner.RestartAsync(cancellationToken);
        return Results.Accepted(value: new { status = "restarted" });
    }

    private static async Task<IResult> StatusCodeAsync(
        int statusCode,
        int delayMs,
        UiStateHub eventHub,
        HttpContext context)
    {
        var line =
            $"{DateTimeOffset.Now:HH:mm:ss.fff} | RESPONDING SERVER: received GET {context.Request.Path}{context.Request.QueryString}; returning HTTP {statusCode} after {delayMs} ms.";
        Console.WriteLine(line);
        eventHub.AddLog($"SERVER: HTTP {statusCode}");
        eventHub.SetApiStatus(statusCode);
        eventHub.AddGraphBar(statusCode);

        if (delayMs > 0)
        {
            await Task.Delay(delayMs, context.RequestAborted);
        }

        return Results.Text($"HTTP {statusCode}", statusCode: statusCode);
    }


    private static async Task WriteEventAsync(HttpContext context, UiState uiState)
    {
        var json = JsonSerializer.Serialize(uiState);
        await context.Response.WriteAsync($"data: {json}\n\n");
        await context.Response.Body.FlushAsync();
    }
}
