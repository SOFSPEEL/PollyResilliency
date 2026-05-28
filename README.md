# PollyResilliency

A small .NET demo showing how to combine Polly and Refit with:

- timeout handling
- exponential-backoff retry
- circuit breaker
- fallback for unavailable downstream APIs

The solution includes a live demo app plus NUnit tests that exercise the resilience behavior end-to-end.

## Projects

- `Resilliency.Comms` - reusable resilience pipeline + typed Refit client registration.
- `Resilliency.Demo` - ASP.NET Core demo app with a browser dashboard and scenario runner.
- `Resilliency.Tests` - NUnit tests validating timeout, retry, circuit-breaker, and fallback behavior.

## Prerequisites

- .NET SDK that supports `net10.0`

## Quick start

From the repository root:

```bash
dotnet restore
```

Run the demo app:

```bash
dotnet run --project Resilliency.Demo/Resilliency.Demo.csproj
```

Then open the URL printed in the console (the app picks an available localhost port automatically).

## Run tests

```bash
dotnet test Resilliency.sln
```

## Demo API endpoints

When the demo is running, these endpoints are available:

- `GET /events` - Server-Sent Events stream for UI updates/logs.
- `POST /run` - starts the predefined resilience scenario.
- `POST /restart` - restarts the running scenario.
- `GET /status/{statusCode}?delayMs={n}` - test endpoint used to simulate responses and latency.

## Using the resilience pipeline in your own DI setup

`Resilliency.Comms` exposes `AddStatusCodeApiComms(...)` to register both:

- `IStatusCodeApi` (Refit client)
- `ResiliencePipeline<HttpResponseMessage>` (Polly pipeline)

Example:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Resilliency.Comms;

var services = new ServiceCollection();

services.AddStatusCodeApiComms(
    baseAddress: new Uri("https://api.example.com"),
    log: Console.WriteLine,
    circuitStateChanged: state => Console.WriteLine($"Circuit is {state}"),
    pipelineLifetime: ServiceLifetime.Singleton,
    options: new ApiResiliencePolicyOptions
    {
        MaxRetryAttempts = 3,
        RetryDelay = TimeSpan.FromMilliseconds(250),
        BreakDuration = TimeSpan.FromSeconds(2),
        Timeout = TimeSpan.FromMilliseconds(250)
    });
```

## Notes

- The fallback response text is defined by `ApiResiliencePolicies.ServerUnavailableFallbackMessage`.
- The demo intentionally logs each resilience event so the policy transitions are easy to follow.

