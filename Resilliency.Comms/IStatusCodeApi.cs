using Refit;

namespace Resilliency.Comms;

public interface IStatusCodeApi
{
    [Get("/status/{statusCode}")]
    Task<HttpResponseMessage> GetStatusAsync(
        int statusCode,
        [Query] int delayMs,
        CancellationToken cancellationToken);
}
