namespace Resilliency.Comms;

public sealed class ApiResiliencePolicyOptions
{
    public double CircuitFailureRatio { get; init; } = 1.0;

    public int MaxRetryAttempts { get; init; } =2;

    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(250);

    public int CircuitMinimumThroughput { get; init; } = 4;

    public TimeSpan SamplingDuration { get; init; } = TimeSpan.FromSeconds(45);

    public TimeSpan BreakDuration { get; init; } = TimeSpan.FromSeconds(2);
}
