using System.Threading.Channels;

namespace Resilliency.Demo;

internal sealed record DemoSubscription(ChannelReader<DemoEvent> Reader, Action Unsubscribe) : IDisposable
{
    public void Dispose() => Unsubscribe();
}
