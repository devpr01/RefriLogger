using System.Diagnostics.CodeAnalysis;

namespace RefriLogger.Models;

// ---------- Internal records that flow through the pipeline ----------
public abstract record IngestRecord;

public sealed record Reading(
    string DeviceId, string Sensor, double Value, string? Unit,
    DateTimeOffset Ts, DateTimeOffset ReceivedTs) : IngestRecord;

public sealed record DeviceEvent(
    string DeviceId, string Type, string Severity, string? Message,
    DateTimeOffset Ts, DateTimeOffset ReceivedTs) : IngestRecord;

// ---------- Wire format (JSON, camelCase) ----------
public sealed record ReadingDto(string? DeviceId, string? Sensor, double? Value, string? Unit, DateTimeOffset? Ts);
public sealed record EventDto(string? DeviceId, string? Type, string? Severity, string? Message, DateTimeOffset? Ts);

// ---------- Query results ----------
public sealed record ReadingRow(string DeviceId, string Sensor, DateTimeOffset Ts, double Value, string? Unit);
public sealed record EventRow(long Id, string DeviceId, string Type, string Severity, string? Message, DateTimeOffset Ts);
public sealed record DeviceRow(string DeviceId, DateTimeOffset LastSeen, int SensorCount);

// ---------- Validation ----------
public static class Validation
{
    private const int MaxId = 64;
    private const int MaxUnit = 16;
    private const int MaxMessage = 512;
    private static readonly TimeSpan MaxFutureSkew = TimeSpan.FromMinutes(5);
    private static readonly HashSet<string> Severities = new(StringComparer.OrdinalIgnoreCase) { "info", "warning", "critical" };

    public static (IngestRecord? Record, string? Error) Reading(ReadingDto? d, DateTimeOffset now)
    {
        if (d is null) return (null, "null item");
        if (!Id(d.DeviceId, "deviceId", out var device, out var err)) return (null, err);
        if (!Id(d.Sensor, "sensor", out var sensor, out err)) return (null, err);
        if (d.Value is not double v || !double.IsFinite(v)) return (null, "value must be a finite number");
        if (d.Unit is { Length: > MaxUnit }) return (null, $"unit too long (max {MaxUnit})");
        var ts = d.Ts ?? now;
        if (ts > now + MaxFutureSkew) return (null, "ts is in the future (check device clock)");
        return (new Models.Reading(device, sensor, v, d.Unit, ts, now), null);
    }

    public static (IngestRecord? Record, string? Error) Event(EventDto? d, DateTimeOffset now)
    {
        if (d is null) return (null, "null item");
        if (!Id(d.DeviceId, "deviceId", out var device, out var err)) return (null, err);
        if (!Id(d.Type, "type", out var type, out err)) return (null, err);
        var sev = string.IsNullOrWhiteSpace(d.Severity) ? "info" : d.Severity.Trim().ToLowerInvariant();
        if (!Severities.Contains(sev)) return (null, "severity must be info, warning or critical");
        if (d.Message is { Length: > MaxMessage }) return (null, $"message too long (max {MaxMessage})");
        var ts = d.Ts ?? now;
        if (ts > now + MaxFutureSkew) return (null, "ts is in the future (check device clock)");
        return (new DeviceEvent(device, type, sev, d.Message, ts, now), null);
    }

    private static bool Id(string? raw, string field, [NotNullWhen(true)] out string? value, [NotNullWhen(false)] out string? error)
    {
        value = raw?.Trim();
        if (string.IsNullOrEmpty(value)) { error = $"{field} is required"; value = null; return false; }
        if (value.Length > MaxId) { error = $"{field} too long (max {MaxId})"; value = null; return false; }
        error = null;
        return true;
    }
}
