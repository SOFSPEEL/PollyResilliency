using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Spotflow.InMemory.Azure.EventHubs;

namespace Resilliency.Comms;

public sealed class EventHubResilienceEventPublisher : IResilienceEventPublisher, IAsyncDisposable
{
    private readonly EventHubProducerClient _producerClient;

    public EventHubResilienceEventPublisher(EventHubProducerClient producerClient)
    {
        _producerClient = producerClient;
    }

    public static EventHubResilienceEventPublisher CreateInMemory(
        string namespaceName = "resilliency-demo",
        string eventHubName = "resilience-events")
    {
        var provider = new InMemoryEventHubProvider();
        var eventHub = provider.AddNamespace(namespaceName).AddEventHub(eventHubName, numberOfPartitions: 1);
        return new EventHubResilienceEventPublisher(InMemoryEventHubProducerClient.FromEventHub(eventHub));
    }

    public async ValueTask PublishAsync(ResilienceEvent resilienceEvent, CancellationToken cancellationToken = default)
    {
        var eventData = new EventData(BinaryData.FromObjectAsJson(resilienceEvent));
        await _producerClient.SendAsync([eventData], cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _producerClient.DisposeAsync();
    }
}
