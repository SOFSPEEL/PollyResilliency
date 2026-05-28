using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Resilliency.Comms;
using Resilliency.Demo;

var port = GetAvailablePort();
var baseAddress = new Uri($"http://localhost:{port}");

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(baseAddress.ToString());

var events = new DemoEventHub();
var demoPolicyOptions = new ApiResiliencePolicyOptions
{
    RetryDelay = TimeSpan.FromSeconds(1),
    BreakDuration = TimeSpan.FromSeconds(8)
};

builder.Services.AddSingleton(events);
builder.Services.AddSingleton(new DemoScenarioOptions(demoPolicyOptions));
builder.Services.AddSingleton<DemoScenarioRunner>();
builder.Services.AddStatusCodeApiComms(
    baseAddress,
    message =>
    {
        var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} | {message}";
        Console.WriteLine(line);
        events.Publish("log", line);
    },
    state => events.Publish("state", $"Circuit is {state}", state.ToString()),
    ServiceLifetime.Transient,
    demoPolicyOptions);

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
