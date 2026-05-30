namespace Resilliency.Comms;

public interface IResilienceEventPublisher
{
    ValueTask PublishAsync(ResilienceEvent resilienceEvent, CancellationToken cancellationToken = default);
}
