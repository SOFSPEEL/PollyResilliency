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

    private static async Task StreamEventsAsync(HttpContext context, DemoEventHub eventHub)
    {
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";
        context.Response.ContentType = "text/event-stream";

        var subscription = eventHub.Subscribe();
        await WriteEventAsync(context, new DemoEvent("state", "Circuit starts closed", "Closed"));

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
        CancellationToken cancellationToken)
    {
        await runner.RestartAsync(cancellationToken);
        return Results.Accepted(value: new { status = "restarted" });
    }

    private static async Task<IResult> StatusCodeAsync(
        int statusCode,
        int delayMs,
        DemoEventHub eventHub,
        HttpContext context)
    {
        var line =
            $"{DateTimeOffset.Now:HH:mm:ss.fff} | API SERVER: received GET {context.Request.Path}{context.Request.QueryString}; returning HTTP {statusCode} after {delayMs} ms.";
        Console.WriteLine(line);
        eventHub.Publish("log", line);
        eventHub.Publish("api-status", $"HTTP {statusCode}", statusCode.ToString());

        if (delayMs > 0)
        {
            await Task.Delay(delayMs, context.RequestAborted);
        }

        return Results.Text($"HTTP {statusCode}", statusCode: statusCode);
    }

    private static async Task WriteEventAsync(HttpContext context, DemoEvent demoEvent)
    {
        var json = JsonSerializer.Serialize(demoEvent);
        await context.Response.WriteAsync($"data: {json}\n\n");
        await context.Response.Body.FlushAsync();
    }
}
