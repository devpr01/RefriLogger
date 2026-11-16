using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using RefriLogger.Core;
using RefriLogger.Models;

namespace RefriLogger.Storage;

public sealed class SqliteStore : ITelemetrySink
{
    private readonly string _connectionString;
    public string Name => "sqlite";

    public SqliteStore(IOptions<StorageOptions> options)
    {
        var path = Path.GetFullPath(options.Value.DbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString();
        Initialize();
    }

    private SqliteConnection Open()
    {
        var c = new SqliteConnection(_connectionString);
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "PRAGMA synchronous=NORMAL;";   // safe with WAL, much faster than FULL
        cmd.ExecuteNonQuery();
        return c;
    }

    private void Initialize()
    {
        using var c = Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = """
            PRAGMA journal_mode=WAL;

            -- PK (device, sensor, ts) = per-device queries are fast AND duplicate resends are ignored (idempotent)
            CREATE TABLE IF NOT EXISTS readings (
                device_id   TEXT    NOT NULL,
                sensor      TEXT    NOT NULL,
                ts          INTEGER NOT NULL,   -- device time, unix ms UTC
                received_ts INTEGER NOT NULL,   -- server time, unix ms UTC
                value       REAL    NOT NULL,
                unit        TEXT,
                PRIMARY KEY (device_id, sensor, ts)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS ix_readings_ts ON readings(ts);

            -- last known value per device+sensor (fast "current state" + future heartbeat/offline detection)
            CREATE TABLE IF NOT EXISTS latest_readings (
                device_id TEXT    NOT NULL,
                sensor    TEXT    NOT NULL,
                ts        INTEGER NOT NULL,
                value     REAL    NOT NULL,
                unit      TEXT,
                PRIMARY KEY (device_id, sensor)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id   TEXT    NOT NULL,
                type        TEXT    NOT NULL,
                severity    TEXT    NOT NULL,
                message     TEXT,
                ts          INTEGER NOT NULL,
                received_ts INTEGER NOT NULL,
                UNIQUE (device_id, type, ts)
            );
            CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts);
            """;
        cmd.ExecuteNonQuery();
    }

    public async Task<bool> PingAsync(CancellationToken ct)
    {
        try
        {
            await using var c = Open();
            await using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT 1";
            await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch { return false; }
    }

    // ------------------------------------------------------------------ write
    public async Task WriteAsync(IReadOnlyList<Reading> readings, IReadOnlyList<DeviceEvent> events, CancellationToken ct)
    {
        await using var c = Open();
        await using var tx = (SqliteTransaction)await c.BeginTransactionAsync(ct);

        if (readings.Count > 0)
        {
            await using var ins = c.CreateCommand();
            ins.Transaction = tx;
            ins.CommandText = "INSERT OR IGNORE INTO readings(device_id,sensor,ts,received_ts,value,unit) VALUES($d,$s,$t,$r,$v,$u)";
            var pd = ins.Parameters.Add("$d", SqliteType.Text);
            var ps = ins.Parameters.Add("$s", SqliteType.Text);
            var pt = ins.Parameters.Add("$t", SqliteType.Integer);
            var pr = ins.Parameters.Add("$r", SqliteType.Integer);
            var pv = ins.Parameters.Add("$v", SqliteType.Real);
            var pu = ins.Parameters.Add("$u", SqliteType.Text);
            foreach (var x in readings)
            {
                pd.Value = x.DeviceId; ps.Value = x.Sensor;
                pt.Value = x.Ts.ToUnixTimeMilliseconds(); pr.Value = x.ReceivedTs.ToUnixTimeMilliseconds();
                pv.Value = x.Value; pu.Value = (object?)x.Unit ?? DBNull.Value;
                await ins.ExecuteNonQueryAsync(ct);
            }

            // upsert only the newest reading per device+sensor of this batch
            await using var up = c.CreateCommand();
            up.Transaction = tx;
            up.CommandText = """
                INSERT INTO latest_readings(device_id,sensor,ts,value,unit) VALUES($d,$s,$t,$v,$u)
                ON CONFLICT(device_id,sensor) DO UPDATE
                  SET ts=excluded.ts, value=excluded.value, unit=excluded.unit
                  WHERE excluded.ts > latest_readings.ts
                """;
            var ud = up.Parameters.Add("$d", SqliteType.Text);
            var us = up.Parameters.Add("$s", SqliteType.Text);
            var ut = up.Parameters.Add("$t", SqliteType.Integer);
            var uv = up.Parameters.Add("$v", SqliteType.Real);
            var uu = up.Parameters.Add("$u", SqliteType.Text);
            foreach (var g in readings.GroupBy(r => (r.DeviceId, r.Sensor)))
            {
                var last = g.MaxBy(r => r.Ts)!;
                ud.Value = last.DeviceId; us.Value = last.Sensor;
                ut.Value = last.Ts.ToUnixTimeMilliseconds(); uv.Value = last.Value;
                uu.Value = (object?)last.Unit ?? DBNull.Value;
                await up.ExecuteNonQueryAsync(ct);
            }
        }

        if (events.Count > 0)
        {
            await using var ins = c.CreateCommand();
            ins.Transaction = tx;
            ins.CommandText = "INSERT OR IGNORE INTO events(device_id,type,severity,message,ts,received_ts) VALUES($d,$y,$sv,$m,$t,$r)";
            var pd = ins.Parameters.Add("$d", SqliteType.Text);
            var py = ins.Parameters.Add("$y", SqliteType.Text);
            var psv = ins.Parameters.Add("$sv", SqliteType.Text);
            var pm = ins.Parameters.Add("$m", SqliteType.Text);
            var pt = ins.Parameters.Add("$t", SqliteType.Integer);
            var pr = ins.Parameters.Add("$r", SqliteType.Integer);
            foreach (var e in events)
            {
                pd.Value = e.DeviceId; py.Value = e.Type; psv.Value = e.Severity;
                pm.Value = (object?)e.Message ?? DBNull.Value;
                pt.Value = e.Ts.ToUnixTimeMilliseconds(); pr.Value = e.ReceivedTs.ToUnixTimeMilliseconds();
                await ins.ExecuteNonQueryAsync(ct);
            }
        }

        await tx.CommitAsync(ct);
    }

    // ------------------------------------------------------------------ read
    public async Task<List<ReadingRow>> QueryReadingsAsync(
        string? device, string? sensor, DateTimeOffset from, DateTimeOffset to, int limit, CancellationToken ct)
    {
        await using var c = Open();
        await using var cmd = c.CreateCommand();
        var sql = new StringBuilder("SELECT device_id,sensor,ts,value,unit FROM readings WHERE ts>=$from AND ts<$to");
        cmd.Parameters.AddWithValue("$from", from.ToUnixTimeMilliseconds());
        cmd.Parameters.AddWithValue("$to", to.ToUnixTimeMilliseconds());
        if (!string.IsNullOrEmpty(device)) { sql.Append(" AND device_id=$d"); cmd.Parameters.AddWithValue("$d", device); }
        if (!string.IsNullOrEmpty(sensor)) { sql.Append(" AND sensor=$s"); cmd.Parameters.AddWithValue("$s", sensor); }
        sql.Append(" ORDER BY ts DESC LIMIT $limit");
        cmd.Parameters.AddWithValue("$limit", limit);
        cmd.CommandText = sql.ToString();

        var list = new List<ReadingRow>();
        await using var rd = await cmd.ExecuteReaderAsync(ct);
        while (await rd.ReadAsync(ct))
            list.Add(new ReadingRow(rd.GetString(0), rd.GetString(1),
                DateTimeOffset.FromUnixTimeMilliseconds(rd.GetInt64(2)), rd.GetDouble(3),
                rd.IsDBNull(4) ? null : rd.GetString(4)));
        return list;
    }

    public async Task<List<ReadingRow>> QueryLatestAsync(string? device, CancellationToken ct)
    {
        await using var c = Open();
        await using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT device_id,sensor,ts,value,unit FROM latest_readings"
                          + (string.IsNullOrEmpty(device) ? "" : " WHERE device_id=$d")
                          + " ORDER BY device_id, sensor";
        if (!string.IsNullOrEmpty(device)) cmd.Parameters.AddWithValue("$d", device);

        var list = new List<ReadingRow>();
        await using var rd = await cmd.ExecuteReaderAsync(ct);
        while (await rd.ReadAsync(ct))
            list.Add(new ReadingRow(rd.GetString(0), rd.GetString(1),
                DateTimeOffset.FromUnixTimeMilliseconds(rd.GetInt64(2)), rd.GetDouble(3),
                rd.IsDBNull(4) ? null : rd.GetString(4)));
        return list;
    }

    public async Task<List<DeviceRow>> QueryDevicesAsync(CancellationToken ct)
    {
        await using var c = Open();
        await using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT device_id, MAX(ts), COUNT(*) FROM latest_readings GROUP BY device_id ORDER BY device_id";
        var list = new List<DeviceRow>();
        await using var rd = await cmd.ExecuteReaderAsync(ct);
        while (await rd.ReadAsync(ct))
            list.Add(new DeviceRow(rd.GetString(0), DateTimeOffset.FromUnixTimeMilliseconds(rd.GetInt64(1)), rd.GetInt32(2)));
        return list;
    }

    public async Task<List<EventRow>> QueryEventsAsync(
        string? device, string? type, string? severity, DateTimeOffset from, DateTimeOffset to, int limit, CancellationToken ct)
    {
        await using var c = Open();
        await using var cmd = c.CreateCommand();
        var sql = new StringBuilder("SELECT id,device_id,type,severity,message,ts FROM events WHERE ts>=$from AND ts<$to");
        cmd.Parameters.AddWithValue("$from", from.ToUnixTimeMilliseconds());
        cmd.Parameters.AddWithValue("$to", to.ToUnixTimeMilliseconds());
        if (!string.IsNullOrEmpty(device)) { sql.Append(" AND device_id=$d"); cmd.Parameters.AddWithValue("$d", device); }
        if (!string.IsNullOrEmpty(type)) { sql.Append(" AND type=$y"); cmd.Parameters.AddWithValue("$y", type); }
        if (!string.IsNullOrEmpty(severity)) { sql.Append(" AND severity=$sv"); cmd.Parameters.AddWithValue("$sv", severity.ToLowerInvariant()); }
        sql.Append(" ORDER BY ts DESC LIMIT $limit");
        cmd.Parameters.AddWithValue("$limit", limit);
        cmd.CommandText = sql.ToString();

        var list = new List<EventRow>();
        await using var rd = await cmd.ExecuteReaderAsync(ct);
        while (await rd.ReadAsync(ct))
            list.Add(new EventRow(rd.GetInt64(0), rd.GetString(1), rd.GetString(2), rd.GetString(3),
                rd.IsDBNull(4) ? null : rd.GetString(4), DateTimeOffset.FromUnixTimeMilliseconds(rd.GetInt64(5))));
        return list;
    }

    // ------------------------------------------------------------------ retention
    public async Task<(int Readings, int Events)> DeleteOlderThanAsync(DateTimeOffset cutoff, CancellationToken ct)
    {
        await using var c = Open();
        var ms = cutoff.ToUnixTimeMilliseconds();
        var r = await DeleteOlderAsync(c, "readings", ms, ct);
        var e = await DeleteOlderAsync(c, "events", ms, ct);
        return (r, e);
    }

    // Deletes in 1-hour slices so the write lock is never held for long.
    private static async Task<int> DeleteOlderAsync(SqliteConnection c, string table, long cutoffMs, CancellationToken ct)
    {
        var total = 0;
        while (!ct.IsCancellationRequested)
        {
            long? min;
            await using (var q = c.CreateCommand())
            {
                q.CommandText = $"SELECT MIN(ts) FROM {table}";   // table name is a constant, not user input
                var o = await q.ExecuteScalarAsync(ct);
                min = o is null or DBNull ? null : Convert.ToInt64(o);
            }
            if (min is null || min >= cutoffMs) break;

            var upper = Math.Min(min.Value + 3_600_000, cutoffMs);
            await using var d = c.CreateCommand();
            d.CommandText = $"DELETE FROM {table} WHERE ts < $u";
            d.Parameters.AddWithValue("$u", upper);
            total += await d.ExecuteNonQueryAsync(ct);
        }
        return total;
    }
}
