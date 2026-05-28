using System.Threading.Channels;

namespace Resilliency.Demo;

internal sealed record UiStateSubscription(ChannelReader<UiState> Reader, Action Unsubscribe) : IDisposable
{
    public void Dispose() => Unsubscribe();
}

