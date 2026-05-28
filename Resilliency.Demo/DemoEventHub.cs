using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Resilliency.Demo;

internal sealed class DemoEventHub
{
    private readonly ConcurrentDictionary<Guid, Channel<DemoEvent>> _subscribers = new();

    public DemoSubscription Subscribe()
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<DemoEvent>();
        _subscribers[id] = channel;
        return new DemoSubscription(channel.Reader, () => _subscribers.TryRemove(id, out _));
    }

    public void Publish(string type, string message, string? state = null)
    {
        var demoEvent = new DemoEvent(type, message, state);

        foreach (var subscriber in _subscribers.Values)
        {
            subscriber.Writer.TryWrite(demoEvent);
        }
    }
}
