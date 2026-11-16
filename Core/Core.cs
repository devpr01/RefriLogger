using System.Threading.Channels;
using Microsoft.Extensions.Options;
using RefriLogger.Models;

namespace RefriLogger.Core;

// ---------- Options ----------
public sealed class StorageOptions
{
    public string DbPath { get; set; } = "data/logger.db";
    public int RetentionDays { get; set; } = 30;   // <= 0 disables retention
}
public sealed class SimulatorOptions
{
    public bool Enabled { get; set; }
    public int Devices { get; set; } = 5;
    public int IntervalSeconds { get; set; } = 5;
}

// ---------- Extension point #1: SINKS (where data goes) ----------
// Add a new destination (MQTT forward, webhook, central DB, CSV...) by implementing this
// and registering it:  services.AddSingleton<ITelemetrySink, MyNewSink>();
public interface ITelemetrySink
{
    string Name { get; }
    Task WriteAsync(IReadOnlyList<Reading> readings, IReadOnlyList<DeviceEvent> events, CancellationToken ct);
}

// ---------- Extension point #2: SOURCES (where data comes from) ----------
// Any class (HTTP endpoint, simulator, future MQTT/Modbus BackgroundService) just calls TryEnqueue.
public sealed class IngestQueue
{
    private readonly Channel<IngestRecord> _channel = Channel.CreateBounded<IngestRecord>(
        new BoundedChannelOptions(100_000)
        {
            FullMode = BoundedChannelFullMode.Wait, // TryWrite returns false when full => caller gets 503
            SingleReader = true
        });

    public bool TryEnqueue(IngestRecord record) => _channel.Writer.TryWrite(record);
    public ChannelReader<IngestRecord> Reader => _channel.Reader;
    public int Count => _channel.Reader.Count;
}

// ---------- Dispatcher: batches records and fans out to all sinks ----------
public sealed class DispatcherService(IngestQueue queue, IEnumerable<ITelemetrySink> sinks, ILogger<DispatcherService> log) : BackgroundService
{
    private const int MaxBatch = 1000;
    private static readonly TimeSpan CoalesceDelay = TimeSpan.FromMilliseconds(200);

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        log.LogInformation("Dispatcher started. Sinks: {Sinks}", string.Join(", ", sinks.Select(s => s.Name)));
        try
        {
            while (await queue.Reader.WaitToReadAsync(stop))
            {
                await Task.Delay(CoalesceDelay, stop);   // let a few more records pile up => bigger batches
                await DrainOnceAsync();
            }
        }
        catch (OperationCanceledException) { /* shutting down */ }

        while (queue.Count > 0) await DrainOnceAsync();  // flush what is left before exit
        log.LogInformation("Dispatcher stopped, queue drained");
    }

    private async Task DrainOnceAsync()
    {
        var readings = new List<Reading>();
        var events = new List<DeviceEvent>();
        var n = 0;
        while (n < MaxBatch && queue.Reader.TryRead(out var rec))
        {
            n++;
            switch (rec)
            {
                case Reading r: readings.Add(r); break;
                case DeviceEvent e: events.Add(e); break;
            }
        }
        if (n == 0) return;

        foreach (var sink in sinks)           // one failing sink must not block the others
            await WriteWithRetryAsync(sink, readings, events);
    }

    private async Task WriteWithRetryAsync(ITelemetrySink sink, List<Reading> readings, List<DeviceEvent> events)
    {
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await sink.WriteAsync(readings, events, CancellationToken.None);
                return;
            }
            catch (Exception ex)
            {
                if (attempt == 3)
                    log.LogError(ex, "Sink {Sink} failed after 3 attempts; dropping {R} readings, {E} events",
                        sink.Name, readings.Count, events.Count);
                else
                {
                    log.LogWarning(ex, "Sink {Sink} failed (attempt {A}); retrying", sink.Name, attempt);
                    await Task.Delay(TimeSpan.FromSeconds(attempt));
                }
            }
        }
    }
}

// ---------- Retention: delete data older than N days ----------
public sealed class RetentionService(
    Storage.SqliteStore store, IOptions<StorageOptions> options, TimeProvider time, ILogger<RetentionService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var days = options.Value.RetentionDays;
        if (days <= 0) { log.LogWarning("Retention disabled (RetentionDays={Days}). Disk will grow unbounded.", days); return; }

        try
        {
            await Task.Delay(TimeSpan.FromMinutes(1), ct);
            using var timer = new PeriodicTimer(TimeSpan.FromHours(1));
            do { await RunOnceAsync(days, ct); } while (await timer.WaitForNextTickAsync(ct));
        }
        catch (OperationCanceledException) { }
    }

    private async Task RunOnceAsync(int days, CancellationToken ct)
    {
        try
        {
            var cutoff = time.GetUtcNow().AddDays(-days);
            var (r, e) = await store.DeleteOlderThanAsync(cutoff, ct);
            if (r + e > 0) log.LogInformation("Retention: removed {R} readings and {E} events older than {Cutoff:u}", r, e, cutoff);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            log.LogError(ex, "Retention run failed");
        }
    }
}
