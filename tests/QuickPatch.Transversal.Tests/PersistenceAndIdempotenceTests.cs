using Microsoft.EntityFrameworkCore;

using QuickPatch.Transversal.Messaging;
using QuickPatch.Transversal.Persistence;

using Testcontainers.PostgreSql;

namespace QuickPatch.Transversal.Tests;

/// <summary>
/// Transacción por tenant e idempotencia contra un PostgreSQL real con RLS: el tenant fijado es el que ve la política,
/// un fallo revierte todo y un evento registrado se reconoce como ya aplicado.
/// </summary>
public sealed class PersistenceAndIdempotenceTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public Task InitializeAsync() => postgres.StartAsync();

    public Task DisposeAsync() => postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task LaTransaccionFijaElTenantQueVeRls()
    {
        await using var db = await CreateAsync();
        var tenant = Guid.NewGuid();

        var visto = await TenantTransaction.ExecuteAsync(db, tenant, async ct =>
            await db.Database.SqlQuery<string>($"SELECT current_setting('app.current_tenant') AS \"Value\"").SingleAsync(ct),
            CancellationToken.None);

        Assert.Equal(tenant.ToString(), visto);
        Assert.Equal("app.current_tenant", TenantTransaction.SettingName);
    }

    [Fact]
    public async Task ConRls_SoloSeVenLasFilasDelTenant()
    {
        await using var db = await CreateAsync();
        var (a, b) = (Guid.NewGuid(), Guid.NewGuid());
        db.Set<ProcessedEventRecord>().AddRange(
            new ProcessedEventRecord { EventId = Guid.NewGuid(), TenantId = a, EventType = "x", ProcessedAt = DateTimeOffset.UtcNow },
            new ProcessedEventRecord { EventId = Guid.NewGuid(), TenantId = b, EventType = "x", ProcessedAt = DateTimeOffset.UtcNow });
        await db.SaveChangesAsync();
        db.ChangeTracker.Clear();
        await db.Database.ExecuteSqlRawAsync("""
            ALTER TABLE processed_events ENABLE ROW LEVEL SECURITY;
            CREATE POLICY por_tenant ON processed_events USING (tenant_id = current_setting('app.current_tenant')::uuid);
            DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app') THEN CREATE ROLE app; END IF; END $$;
            GRANT SELECT ON processed_events TO app;
            """);

        var filas = await TenantTransaction.ExecuteAsync(db, a, async ct =>
        {
            await SessionRole.SetAsync(db, "app", ct);
            return await db.Set<ProcessedEventRecord>().Select(x => x.TenantId).ToListAsync(ct);
        }, CancellationToken.None);

        Assert.Equal([a], filas);
    }

    [Fact]
    public async Task SiElTrabajoFalla_NoSeGuardaNada()
    {
        await using var db = await CreateAsync();
        var store = new ProcessedEventStore<StoreDbContext>(db);
        var evento = Guid.NewGuid();

        await Assert.ThrowsAsync<InvalidOperationException>(() => TenantTransaction.ExecuteAsync<bool>(db, Guid.NewGuid(), _ =>
        {
            store.Add(evento, Guid.NewGuid(), "x.created", DateTimeOffset.UtcNow);
            throw new InvalidOperationException("falla");
        }, CancellationToken.None));

        db.ChangeTracker.Clear();
        Assert.False(await store.ExistsAsync(evento, CancellationToken.None));
    }

    [Fact]
    public async Task UnErrorAlGuardar_SaleSinTraducir()
    {
        await using var db = await CreateAsync();
        var evento = Guid.NewGuid();
        var store = new ProcessedEventStore<StoreDbContext>(db);
        await TenantTransaction.ExecuteAsync(db, Guid.NewGuid(), _ =>
        {
            store.Add(evento, Guid.NewGuid(), "x.created", DateTimeOffset.UtcNow);
            return Task.FromResult(true);
        }, CancellationToken.None);
        db.ChangeTracker.Clear();

        await Assert.ThrowsAsync<DbUpdateException>(() => TenantTransaction.ExecuteAsync(db, Guid.NewGuid(), _ =>
        {
            store.Add(evento, Guid.NewGuid(), "x.created", DateTimeOffset.UtcNow);
            return Task.FromResult(true);
        }, CancellationToken.None));
    }

    [Fact]
    public async Task UnEventoRegistrado_SeReconoceComoAplicado()
    {
        await using var db = await CreateAsync();
        var store = new ProcessedEventStore<StoreDbContext>(db);
        var (evento, tenant) = (Guid.NewGuid(), Guid.NewGuid());

        Assert.False(await store.ExistsAsync(evento, CancellationToken.None));
        await TenantTransaction.ExecuteAsync(db, tenant, _ =>
        {
            store.Add(evento, tenant, "x.created", DateTimeOffset.UtcNow);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.True(await store.ExistsAsync(evento, CancellationToken.None));
        var fila = await db.Set<ProcessedEventRecord>().SingleAsync(x => x.EventId == evento);
        Assert.Equal((tenant, "x.created"), (fila.TenantId, fila.EventType));
    }

    [Fact]
    public async Task ElTenantVacioSeRechaza()
    {
        await using var db = new StoreDbContext(new DbContextOptionsBuilder<StoreDbContext>().UseNpgsql("Host=no-existe").Options);

        await Assert.ThrowsAsync<ArgumentException>(() => TenantTransaction.ExecuteAsync(db, Guid.Empty, _ => Task.FromResult(1), CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(() => TenantTransaction.SetTenantAsync(db, Guid.Empty, CancellationToken.None));
    }

    [Fact]
    public async Task ElOutboxSeGuardaConElMapeoComun()
    {
        await using var db = await CreateAsync();
        var record = OutboxEventRecord.Pending(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), "x.created", "{\"a\":1}", DateTimeOffset.UtcNow);
        db.Set<OutboxEventRecord>().Add(record);
        await db.SaveChangesAsync();
        db.ChangeTracker.Clear();

        var fila = await db.Set<OutboxEventRecord>().SingleAsync();
        Assert.Equal((record.Id, record.TenantId, record.AggregateId, "x.created", 0), (fila.Id, fila.TenantId, fila.AggregateId, fila.EventType, fila.Attempts));
        Assert.Null(fila.PublishedAt);
        var indice = db.Model.FindEntityType(typeof(OutboxEventRecord))!.GetIndexes().Single();
        Assert.Equal(("ix_outbox_events_pending", "published_at IS NULL"), (indice.GetDatabaseName(), indice.GetFilter()));
    }

    private async Task<StoreDbContext> CreateAsync()
    {
        var db = new StoreDbContext(new DbContextOptionsBuilder<StoreDbContext>().UseNpgsql(postgres.GetConnectionString()).Options);
        await db.Database.EnsureCreatedAsync();
        return db;
    }
}

public sealed class StoreDbContext(DbContextOptions<StoreDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.MapOutboxEvents().MapProcessedEvents();
}