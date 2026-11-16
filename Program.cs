using RefriLogger.Api;
using RefriLogger.Core;
using RefriLogger.Simulation;
using RefriLogger.Storage;

var builder = WebApplication.CreateBuilder(args);

// systemd integration: Type=notify readiness + journald-friendly log format. No-op outside systemd.
builder.Host.UseSystemd();

builder.WebHost.ConfigureKestrel(o =>
{
    o.AddServerHeader = false;
    o.Limits.MaxRequestBodySize = 2 * 1024 * 1024;
});

builder.Services.Configure<StorageOptions>(builder.Configuration.GetSection("Storage"));
builder.Services.Configure<SimulatorOptions>(builder.Configuration.GetSection("Simulator"));

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IngestQueue>();

// Storage = one sink. Add more sinks the same way (AddSingleton<ITelemetrySink, YourSink>()).
builder.Services.AddSingleton<SqliteStore>();
builder.Services.AddSingleton<ITelemetrySink>(sp => sp.GetRequiredService<SqliteStore>());

builder.Services.AddHostedService<DispatcherService>();
builder.Services.AddHostedService<RetentionService>();
builder.Services.AddHostedService<SensorSimulator>();   // only active when Simulator:Enabled=true

var app = builder.Build();
app.MapApi();
app.Run();
