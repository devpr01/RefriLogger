using System.Security.Cryptography;
using System.Text;
using RefriLogger.Core;
using RefriLogger.Models;
using RefriLogger.Storage;

namespace RefriLogger.Api;

public static class ApiEndpoints
{
    private const int MaxBatch = 5000;
    private const int DefaultLimit = 1000;
    private const int MaxLimit = 10_000;

    public static void MapApi(this WebApplication app)
    {
        // Unauthenticated: for systemd/monitoring probes
        app.MapGet("/health", async (SqliteStore store, IngestQueue queue, CancellationToken ct) =>
        {
            var dbOk = await store.PingAsync(ct);
            var body = new { status = dbOk ? "ok" : "degraded", db = dbOk, queueDepth = queue.Count };
            return dbOk ? Results.Ok(body) : Results.Json(body, statusCode: StatusCodes.Status503ServiceUnavailable);
        });

        var api = app.MapGroup("/api/v1");

        // Optional shared API key (LAN-only for now). Empty key = no auth.
        var key = app.Configuration["Security:ApiKey"];
        if (!string.IsNullOrEmpty(key))
        {
            var keyBytes = Encoding.UTF8.GetBytes(key);
            api.AddEndpointFilter(async (ctx, next) =>
            {
                var supplied = ctx.HttpContext.Request.Headers["X-Api-Key"].ToString();
                if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(supplied), keyBytes))
                    return Results.Unauthorized();
                return await next(ctx);
            });
        }

        // ---------------- ingest ----------------
        api.MapPost("/readings", (ReadingDto[] items, IngestQueue q, TimeProvider tp) =>
            Ingest(items, q, tp, (d, now) => Validation.Reading(d, now)));

        api.MapPost("/events", (EventDto[] items, IngestQueue q, TimeProvider tp) =>
            Ingest(items, q, tp, (d, now) => Validation.Event(d, now)));

        // ---------------- query ----------------
        api.MapGet("/devices", async (SqliteStore s, CancellationToken ct) =>
            Results.Ok(await s.QueryDevicesAsync(ct)));

        api.MapGet("/readings/latest", async (SqliteStore s, string? deviceId, CancellationToken ct) =>
            Results.Ok(await s.QueryLatestAsync(deviceId, ct)));

        api.MapGet("/readings", async (SqliteStore s, TimeProvider tp, string? deviceId, string? sensor,
            DateTimeOffset? @from, DateTimeOffset? to, int? limit, CancellationToken ct) =>
        {
            var (f, t, err) = Range(tp, @from, to);
            if (err is not null) return Results.BadRequest(new { error = err });
            return Results.Ok(await s.QueryReadingsAsync(deviceId, sensor, f, t, ClampLimit(limit), ct));
        });

        api.MapGet("/events", async (SqliteStore s, TimeProvider tp, string? deviceId, string? type, string? severity,
            DateTimeOffset? @from, DateTimeOffset? to, int? limit, CancellationToken ct) =>
        {
            var (f, t, err) = Range(tp, @from, to);
            if (err is not null) return Results.BadRequest(new { error = err });
            return Results.Ok(await s.QueryEventsAsync(deviceId, type, severity, f, t, ClampLimit(limit), ct));
        });
    }

    private static int ClampLimit(int? limit) => Math.Clamp(limit ?? DefaultLimit, 1, MaxLimit);

    // Defaults: last 1 hour. 'to' is exclusive.
    private static (DateTimeOffset From, DateTimeOffset To, string? Error) Range(TimeProvider tp, DateTimeOffset? from, DateTimeOffset? to)
    {
        var t = to ?? tp.GetUtcNow().AddMinutes(5);
        var f = from ?? t.AddHours(-1);
        return f >= t ? (f, t, "'from' must be earlier than 'to'") : (f, t, null);
    }

    private static IResult Ingest<T>(T[] items, IngestQueue q, TimeProvider tp,
        Func<T?, DateTimeOffset, (IngestRecord? Record, string? Error)> build) where T : class
    {
        if (items.Length == 0) return Results.BadRequest(new { error = "empty batch" });
        if (items.Length > MaxBatch) return Results.BadRequest(new { error = $"batch too large (max {MaxBatch})" });

        var now = tp.GetUtcNow();
        var accepted = 0;
        var rejected = new List<object>();

        for (var i = 0; i < items.Length; i++)
        {
            var (rec, err) = build(items[i], now);
            if (rec is null) { rejected.Add(new { index = i, error = err }); continue; }

            // Queue full: tell the device to retry. Resends are safe because writes are idempotent.
            if (!q.TryEnqueue(rec))
                return Results.Json(new { error = "queue full, retry later", accepted, rejected },
                    statusCode: StatusCodes.Status503ServiceUnavailable);
            accepted++;
        }

        if (accepted == 0) return Results.BadRequest(new { accepted, rejected });
        return Results.Accepted(value: new { accepted, rejected });
    }
}
