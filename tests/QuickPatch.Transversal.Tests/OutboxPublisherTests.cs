using System.Reflection;
using System.Text;

using Confluent.Kafka;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using QuickPatch.Transversal.Messaging;
using QuickPatch.Transversal.Persistence;

using Testcontainers.PostgreSql;

namespace QuickPatch.Transversal.Tests;

/// <summary>
/// El publicador contra un PostgreSQL real (Testcontainers): lee con <c>FOR UPDATE SKIP LOCKED</c>, publica en orden,
/// marca <c>published_at</c> y, si Kafka falla, suma el intento y deja el resto pendiente.
/// </summary>
public sealed class OutboxPublisherTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public Task InitializeAsync() => postgres.StartAsync();

    public Task DisposeAsync() => postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task PublicaLosPendientesEnOrdenYLosMarca()
    {
        await using var services = await CreateServicesAsync();
        var (primero, segundo) = await SeedAsync(services);
        var producer = FakeProducer.Create();

        var publicados = await Publisher(services, producer).PublishPendingAsync(CancellationToken.None);

        Assert.Equal(2, publicados);
        Assert.Equal([primero, segundo], FakeProducer.Sent(producer).Select(m => Encoding.UTF8.GetString(m.Headers.GetLastBytes("eventId"))).Select(Guid.Parse));
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.All(await db.OutboxEvents.ToListAsync(), r => Assert.NotNull(r.PublishedAt));
    }

    [Fact]
    public async Task SiKafkaFalla_SumaElIntentoYDejaElLotePendiente()
    {
        await using var services = await CreateServicesAsync();
        var (primero, _) = await SeedAsync(services);
        var producer = FakeProducer.Create(fail: true);

        var publicados = await Publisher(services, producer).PublishPendingAsync(CancellationToken.None);

        Assert.Equal(0, publicados);
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var filas = await db.OutboxEvents.OrderBy(r => r.CreatedAt).ToListAsync();
        Assert.All(filas, r => Assert.Null(r.PublishedAt));
        Assert.Equal(1, filas.Single(r => r.Id == primero).Attempts);
        Assert.Equal(0, filas.Single(r => r.Id != primero).Attempts);
    }

    [Fact]
    public async Task UnRolInvalido_NoSeEjecuta()
    {
        await using var services = await CreateServicesAsync();
        var publisher = new OutboxPublisher<TestDbContext, TestOutboxRecord>(
            services.GetRequiredService<IServiceScopeFactory>(), FakeProducer.Create(),
            Options.Create(new OutboxOptions { PublisherRole = "rol\"; DROP TABLE outbox_events; --" }),
            TimeProvider.System, NullLogger<OutboxPublisher<TestDbContext, TestOutboxRecord>>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishPendingAsync(CancellationToken.None));
    }

    [Fact]
    public void ElMensajeLlevaLaClaveDelAgregadoYLasCabeceras()
    {
        var record = new TestOutboxRecord { Id = Guid.NewGuid(), AggregateId = Guid.NewGuid(), EventType = "x.created", Payload = "{\"a\":1}" };

        var message = OutboxMessages.ToKafkaMessage(record);

        Assert.Equal(record.AggregateId.ToString(), message.Key);
        Assert.Equal(record.Payload, message.Value);
        Assert.Equal(record.Id.ToString(), Encoding.UTF8.GetString(message.Headers.GetLastBytes("eventId")));
        Assert.Equal("x.created", Encoding.UTF8.GetString(message.Headers.GetLastBytes("eventType")));
    }

    [Fact]
    public void LasOpcionesTienenLosValoresPorDefectoDeLosServicios()
    {
        var options = new OutboxOptions();
        Assert.Equal("Outbox", OutboxOptions.Section);
        Assert.Equal(string.Empty, options.PublisherRole);
        Assert.Equal(50, options.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(1), options.PollInterval);
    }

    [Fact]
    public async Task ElServicioEnSegundoPlanPublicaYSeDetieneSinError()
    {
        await using var services = await CreateServicesAsync();
        await SeedAsync(services);
        var producer = FakeProducer.Create();
        var publisher = Publisher(services, producer);

        await publisher.StartAsync(CancellationToken.None);
        for (var i = 0; i < 50 && FakeProducer.Sent(producer).Count < 2; i++)
        {
            await Task.Delay(100);
        }

        await publisher.StopAsync(CancellationToken.None);
        Assert.Equal(2, FakeProducer.Sent(producer).Count);
    }

    [Theory]
    [InlineData("catalog_outbox_publisher", true)]
    [InlineData("_rol9", true)]
    [InlineData("Rol", false)]
    [InlineData("rol-con-guion", false)]
    [InlineData("9rol", false)]
    public void ElNombreDelRolSeValida(string role, bool valid) => Assert.Equal(valid, SessionRole.IsValidName(role));

    [Fact]
    public async Task SinRolNoCambiaDeRol()
    {
        await using var db = new TestDbContext(new DbContextOptionsBuilder<TestDbContext>().UseNpgsql("Host=no-existe").Options);
        await SessionRole.SetAsync(db, " ", CancellationToken.None);
    }

    private static OutboxPublisher<TestDbContext, TestOutboxRecord> Publisher(ServiceProvider services, IProducer<string, string> producer) =>
        new(services.GetRequiredService<IServiceScopeFactory>(), producer,
            Options.Create(new OutboxOptions { PublisherRole = "outbox_publisher", PollInterval = TimeSpan.FromMilliseconds(50) }),
            TimeProvider.System, NullLogger<OutboxPublisher<TestDbContext, TestOutboxRecord>>.Instance);

    private async Task<ServiceProvider> CreateServicesAsync()
    {
        var services = new ServiceCollection()
            .AddDbContext<TestDbContext>(o => o.UseNpgsql(postgres.GetConnectionString()))
            .BuildServiceProvider();
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        await db.Database.EnsureCreatedAsync();
        await db.Database.ExecuteSqlRawAsync("""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'outbox_publisher') THEN CREATE ROLE outbox_publisher; END IF;
            END $$;
            GRANT SELECT, UPDATE ON outbox_events TO outbox_publisher;
            """);
        return services;
    }

    private static async Task<(Guid First, Guid Second)> SeedAsync(ServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var ahora = DateTimeOffset.UtcNow;
        var primero = new TestOutboxRecord { Id = Guid.NewGuid(), AggregateId = Guid.NewGuid(), EventType = "x.created", Payload = "{}", CreatedAt = ahora.AddSeconds(-2) };
        var segundo = new TestOutboxRecord { Id = Guid.NewGuid(), AggregateId = Guid.NewGuid(), EventType = "x.updated", Payload = "{}", CreatedAt = ahora.AddSeconds(-1) };
        db.OutboxEvents.AddRange(segundo, primero);
        await db.SaveChangesAsync();
        return (primero.Id, segundo.Id);
    }
}

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    public DbSet<TestOutboxRecord> OutboxEvents => Set<TestOutboxRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.Entity<TestOutboxRecord>(e =>
        {
            e.ToTable("outbox_events");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.AggregateId).HasColumnName("aggregate_id");
            e.Property(x => x.EventType).HasColumnName("event_type");
            e.Property(x => x.Payload).HasColumnName("payload");
            e.Property(x => x.CreatedAt).HasColumnName("created_at");
            e.Property(x => x.PublishedAt).HasColumnName("published_at");
            e.Property(x => x.Attempts).HasColumnName("attempts");
        });
    }
}

public sealed class TestOutboxRecord : IOutboxRecord
{
    public Guid Id { get; set; }

    public Guid AggregateId { get; set; }

    public string EventType { get; set; } = string.Empty;

    public string Payload { get; set; } = string.Empty;

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset? PublishedAt { get; set; }

    public int Attempts { get; set; }
}

/// <summary>Productor de Kafka en memoria: solo atiende <c>ProduceAsync</c>; puede fallar siempre.</summary>
public class FakeProducer : DispatchProxy
{
    private readonly List<Message<string, string>> sent = [];
    private bool fail;

    public static IProducer<string, string> Create(bool fail = false)
    {
        var producer = DispatchProxy.Create<IProducer<string, string>, FakeProducer>();
        ((FakeProducer)(object)producer).fail = fail;
        return producer;
    }

    public static IReadOnlyList<Message<string, string>> Sent(IProducer<string, string> producer) =>
        ((FakeProducer)(object)producer).sent;

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod);
        ArgumentNullException.ThrowIfNull(args);
        if (targetMethod.Name == nameof(IProducer<string, string>.ProduceAsync) && args[1] is Message<string, string> message)
        {
            if (fail)
            {
                return Task.FromException<DeliveryResult<string, string>>(
                    new ProduceException<string, string>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<string, string> { Message = message }));
            }

            sent.Add(message);
            return Task.FromResult(new DeliveryResult<string, string> { Message = message, Status = PersistenceStatus.Persisted });
        }

        if (targetMethod.Name == "Dispose")
        {
            return null;
        }

        throw new NotSupportedException(targetMethod.Name);
    }
}