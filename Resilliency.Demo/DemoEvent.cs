namespace Resilliency.Demo;

internal sealed record DemoEvent(string Type, string Message, string? State = null, string? GraphLabel = null);
