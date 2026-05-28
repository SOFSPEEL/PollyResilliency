namespace Resilliency.Comms;

public sealed class ApiResiliencePolicyOptions
{
    public int MaxRetryAttempts { get; init; } = 3;

    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(250);

    public TimeSpan BreakDuration { get; init; } = TimeSpan.FromSeconds(2);
}
