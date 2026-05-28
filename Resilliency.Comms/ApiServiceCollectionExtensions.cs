using Microsoft.Extensions.DependencyInjection;
using Refit;

namespace Resilliency.Comms;

public static class ApiServiceCollectionExtensions
{
    public static IServiceCollection AddStatusCodeApiComms(
        this IServiceCollection services,
        Uri baseAddress,
        Action<string> log,
        Action<CircuitBreakerVisualState>? circuitStateChanged = null,
        ServiceLifetime pipelineLifetime = ServiceLifetime.Singleton,
        ApiResiliencePolicyOptions? options = null)
    {
        services.Add(
            new ServiceDescriptor(
                typeof(Polly.ResiliencePipeline<HttpResponseMessage>),
                _ => ApiResiliencePolicies.Create(log, circuitStateChanged, options),
                pipelineLifetime));
        services
            .AddRefitClient<IStatusCodeApi>()
            .ConfigureHttpClient(client => client.BaseAddress = baseAddress);

        return services;
    }
}
