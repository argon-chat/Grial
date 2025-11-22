using System.Text;
using Grial.Core.Clocks;
using Grial.Core.KV;
using Grial.Core.KV.Leases;
using Grial.Core.Storage;
using Grial.Core.WAL;
using Grial.Discovery;
using Grial.Service;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services
    .AddSingleton(_ => new SegmentedLogStorage("./wal"))
    .AddSingleton(_ => new HybridLogicalClock("ru-3"))
    .AddSingleton(_ => TimeProvider.System)
    .AddSingleton<SnapshotManager>()
    .AddSingleton<ILeaseSnapshotBackend, LeaseSnapshotBackend>()
    .AddSingleton<ChangeLog>()
    .AddSingleton<KvWatchManager>()
    .AddSingleton<KvWatchService>()
    .AddSingleton<ReplicatedKvStore>()
    .AddSingleton<LeaseManager>()
    .AddSingleton<LeaseKeyIndex>()
    .AddSingleton<LeaseExpirationHandler>()
    .AddSingleton<EphemeralSessionStore>()
    .AddSingleton<ServiceRegistry>()
    .AddSingleton<SnapshotEffluentScheduler>()
    .AddSingleton<SnapshotEffluentSchedulerOptions>()
    .AddSingleton<WalSchedulerOptions>()
    .AddSingleton<WalScheduler>()
    .AddSingleton<ArgonKvClient>()
    .AddHostedService<WalSchedulerRunner>();


var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();


app.MapGet("/", () => "Grial KV API OK");

app.MapGet("/kv/{key}", ([FromRoute] string key, [FromServices] ReplicatedKvStore kv) =>
{
    if (kv.TryGet(key, out var value))
        return Results.Ok(Encoding.UTF8.GetString(value.Span));

    return Results.NotFound();
});

app.MapPost("/kv/{key}", async ([FromRoute] string key, [FromBody] string value, [FromServices] ArgonKvClient kv) =>
{
    await kv.SetAsync(key, Encoding.UTF8.GetBytes(value));
    return Results.Ok();
});

app.MapPost("/kv/{key}/ensure", async ([FromRoute] string key, [FromBody] string value, [FromServices] ArgonKvClient kv) => {

    foreach (var i in Enumerable.Range(0, 1000))
    {
        await kv.SetAsync($"{key}/{i}", Encoding.UTF8.GetBytes(value));
    }
    return Results.Ok();
});

app.MapPost("/lease", ([FromServices] LeaseManager leases, [FromQuery] TimeSpan ttl) =>
{
    var leaseId = leases.CreateLease(ttl);
    return Results.Ok(new { leaseId = leaseId });
});

app.MapPost("/lease/{id}/renew", ([FromRoute] long id, [FromServices] LeaseManager leases, [FromQuery] TimeSpan ttl) =>
{
    var lid = new LeaseId(id);
    if (!leases.TryRenew(lid, ttl, out var entry))
        return Results.NotFound();

    return Results.Ok(new { leaseId = entry });
});

app.MapDelete("/lease/{id}", (long id, LeaseManager leases) =>
{
    var lid = new LeaseId(id);
    return Results.Ok(leases.TryRelease(lid));
});

app.MapPost("/lease/{id}/attach/{key}", (long id, string key, LeaseKeyIndex index) =>
{
    var lid = new LeaseId(id);
    var bytes = System.Text.Encoding.UTF8.GetBytes(key);

    index.AttachKey(lid, bytes);
    return Results.Ok();
});

app.MapPost("/lease/{id}/detach/{key}", (long id, string key, LeaseKeyIndex index) =>
{
    var lid = new LeaseId(id);
    var bytes = System.Text.Encoding.UTF8.GetBytes(key);

    index.DetachKey(lid, bytes);
    return Results.Ok();
});

app.MapGet("/lease/{id}/keys", (long id, LeaseKeyIndex index) =>
{
    var lid = new LeaseId(id);
    var arr = index.TakeKeysForLease(lid);
    return Results.Ok(arr.Select(x => (string)x).ToArray());
});

app.MapGet("/watch/{key}", async (string key, HttpResponse response, KvWatchService watch) =>
{
    response.Headers.Append("Content-Type", "text/event-stream");

    await foreach (var evt in watch.WatchPrefix(key, 0))
    {
        var json = System.Text.Json.JsonSerializer.Serialize(evt);
        await response.WriteAsync($"data: {json}\n\n");
        await response.Body.FlushAsync();
    }
});

app.MapGet("/kv/load", async ([FromServices] ReplicatedKvStore store, [FromServices] ChangeLog changelog) =>
{
    var handler = new ApplyHandler(store);
    changelog.ScanFrom(0, int.MaxValue, ref handler);
});

app.Run();