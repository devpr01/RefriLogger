using Microsoft.Extensions.Options;
using RefriLogger.Core;
using RefriLogger.Models;

namespace RefriLogger.Simulation;

/// <summary>
/// Fake cold-room devices that feed the same IngestQueue the HTTP API uses.
/// Enable with Simulator:Enabled=true. Remove (or set false) when real sensors are connected.
/// </summary>
public sealed class SensorSimulator(IngestQueue queue, IOptions<SimulatorOptions> options, TimeProvider time, ILogger<SensorSimulator> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var o = options.Value;
        if (!o.Enabled) return;

        var rnd = new Random();
        var devices = Enumerable.Range(1, Math.Max(1, o.Devices))
            .Select(i => new SimDevice($"coldroom-{i:00}", setpoint: i % 2 == 0 ? -18.0 : 4.0, rnd))
            .ToList();

        log.LogInformation("Simulator running: {N} devices, every {S}s", devices.Count, o.IntervalSeconds);
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(Math.Max(1, o.IntervalSeconds)));
            while (await timer.WaitForNextTickAsync(ct))
            {
                var now = time.GetUtcNow();
                foreach (var d in devices) d.Step(now, queue);
            }
        }
        catch (OperationCanceledException) { }
    }

    private sealed class SimDevice
    {
        private readonly Random _rnd;
        private readonly double _setpoint;
        private double _temp;
        private bool _compressorOn = true;
        private int _doorTicks;      // >0 while door is open
        private int _faultTicks;     // >0 while compressor is faulted
        private bool _highTempAlarm;

        public string Id { get; }

        public SimDevice(string id, double setpoint, Random rnd)
        {
            Id = id; _setpoint = setpoint; _temp = setpoint; _rnd = rnd;
        }

        private double Gauss()
        {
            var u1 = 1.0 - _rnd.NextDouble();
            var u2 = _rnd.NextDouble();
            return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
        }

        public void Step(DateTimeOffset now, IngestQueue q)
        {
            void Ev(string type, string sev, string msg) => q.TryEnqueue(new DeviceEvent(Id, type, sev, msg, now, now));
            void Rd(string sensor, double v, string unit) => q.TryEnqueue(new Reading(Id, sensor, Math.Round(v, 2), unit, now, now));

            // door: ~1% chance per tick to open for 3-7 ticks
            if (_doorTicks == 0 && _rnd.NextDouble() < 0.01) { _doorTicks = _rnd.Next(3, 8); Ev("DoorOpen", "info", "Door opened"); }
            else if (_doorTicks > 0 && --_doorTicks == 0) Ev("DoorClosed", "info", "Door closed");

            // compressor fault: ~0.2% chance per tick, lasts 30-60 ticks
            if (_faultTicks == 0 && _rnd.NextDouble() < 0.002)
            {
                _faultTicks = _rnd.Next(30, 60); _compressorOn = false;
                Ev("CompressorFault", "critical", "Compressor stopped unexpectedly");
            }
            else if (_faultTicks > 0 && --_faultTicks == 0) Ev("CompressorRecovered", "info", "Compressor back to normal");

            // thermostat with +/-1 degree hysteresis (disabled while faulted)
            if (_faultTicks == 0)
            {
                if (_temp > _setpoint + 1.0) _compressorOn = true;
                else if (_temp < _setpoint - 1.0) _compressorOn = false;
            }

            _temp += _compressorOn ? -0.25 : 0.12;
            if (_doorTicks > 0) _temp += 0.6;
            var measured = _temp + Gauss() * 0.05;

            // device-side high temp alarm with clear threshold
            if (!_highTempAlarm && measured > _setpoint + 5) { _highTempAlarm = true; Ev("HighTemp", "critical", $"Temperature {measured:F1} C above limit"); }
            else if (_highTempAlarm && measured < _setpoint + 3) { _highTempAlarm = false; Ev("HighTempCleared", "info", "Temperature back in range"); }

            Rd("temperature", measured, "C");
            Rd("humidity", 60 + (_doorTicks > 0 ? 10 : 0) + Gauss() * 1.5, "%");
            Rd("door_open", _doorTicks > 0 ? 1 : 0, "bool");
            Rd("compressor_current", _compressorOn ? 6 + Gauss() * 0.2 : 0.1, "A");
        }
    }
}
