# RefriLogger

Lightweight telemetry/event logger for refrigeration monitoring. ASP.NET Core (.NET 10) minimal API + SQLite, runs as a systemd service on Ubuntu.

## Build (on a dev machine with .NET 10 SDK)

    dotnet publish -c Release -r linux-x64 --self-contained -o publish
    # ARM board (e.g. Raspberry Pi): -r linux-arm64

Copy `publish/` and `deploy/` to the server, then `./deploy/install.sh`.
Self-contained = no .NET runtime needed on Ubuntu.

## Run locally

    dotnet run          # simulator is ON by default, API on http://0.0.0.0:5080

## Operate

    systemctl status refrilogger
    journalctl -u refrilogger -f
    curl http://localhost:5080/health
    sudo ufw allow from 192.168.1.0/24 to any port 5080 proto tcp   # LAN only (adjust subnet)

Config: `appsettings.json` or env vars in `/etc/refrilogger/refrilogger.env`
(e.g. `Simulator__Enabled=false`, `Security__ApiKey=secret`, `Storage__RetentionDays=14`).
If `Security:ApiKey` is set, send header `X-Api-Key` on all `/api/v1/*` calls.

## API

POST /api/v1/readings  (JSON array, max 5000)

    [
      {"deviceId":"coldroom-01","sensor":"temperature","value":3.8,"unit":"C","ts":"2026-11-16T10:00:00Z"},
      {"deviceId":"coldroom-01","sensor":"humidity","value":62,"unit":"%"}
    ]

POST /api/v1/events  (JSON array)

    [{"deviceId":"coldroom-01","type":"DoorOpen","severity":"warning","message":"Door open > 2 min","ts":"2026-11-16T10:00:05Z"}]

`ts` is optional (defaults to server time), must be ISO-8601 UTC. `severity` = info | warning | critical.
Resending the same (deviceId, sensor, ts) is ignored, so retries are safe.
Responses: 202 accepted, 400 all invalid, 503 queue full (retry).

GET  /api/v1/devices
GET  /api/v1/readings/latest?deviceId=coldroom-01
GET  /api/v1/readings?deviceId=&sensor=temperature&from=2026-11-16T00:00:00Z&to=2026-11-16T12:00:00Z&limit=1000
GET  /api/v1/events?deviceId=&type=&severity=critical&from=&to=&limit=
GET  /health

Use `Z` (not `+00:00`) in query-string timestamps, or URL-encode the `+`.

## Adding features later (plug and play)

* New data destination (MQTT forward, webhook, cloud DB): implement `ITelemetrySink`, register with `AddSingleton<ITelemetrySink, X>()`.
* New data source (MQTT subscriber, Modbus poller): a `BackgroundService` that calls `IngestQueue.TryEnqueue(...)`.
* Alerting / offline-device detection: a `BackgroundService` reading `latest_readings` and enqueuing `DeviceEvent`s.
