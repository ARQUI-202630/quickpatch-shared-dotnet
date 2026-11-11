using Confluent.Kafka;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using QuickPatch.Transversal.Persistence;

namespace QuickPatch.Transversal.Messaging;

/// <summary>
/// Publicador del Transactional Outbox (ADR-007). Lee los eventos pendientes de <c>outbox_events</c> con el rol
/// del publicador (DD, sección 10.2), los publica en el topic con el nombre del evento y la clave del agregado, y
/// marca <c>published_at</c>. Si Kafka falla, suma un intento y reintenta en la siguiente vuelta: ningún evento se pierde.
/// </summary>
/// <typeparam name="TContext">DbContext del servicio, dueño de la tabla <c>outbox_events</c>.</typeparam>
/// <typeparam name="TRecord">Clase con la que el servicio mapea la fila de <c>outbox_events</c>.</typeparam>
public sealed partial class OutboxPublisher<TContext, TRecord>(
    IServiceScopeFactory scopes,
    IProducer<string, string> producer,
    IOptions<OutboxOptions> options,
    TimeProvider clock,
    ILogger<OutboxPublisher<TContext, TRecord>> logger) : BackgroundService
    where TContext : DbContext
    where TRecord : class, IOutboxRecord
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var published = await PublishPendingAsync(stoppingToken).ConfigureAwait(false);
                if (published > 0)
                {
                    continue;
                }
            }
            // Solo es un apagado si el token del servicio está cancelado; un tiempo de espera con forma de cancelación
            // se reintenta en la siguiente vuelta en vez de apagar el host.
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                LogPublishFailed(logger, ex);
            }

            await Task.Delay(options.Value.PollInterval, clock, stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>Publica un lote de eventos pendientes; devuelve cuántos quedaron publicados.</summary>
    public async Task<int> PublishPendingAsync(CancellationToken cancellationToken)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var db = scope.ServiceProvider.GetRequiredService<TContext>();
            var transaction = await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using (transaction.ConfigureAwait(false))
            {
                await SessionRole.SetAsync(db, options.Value.PublisherRole, cancellationToken).ConfigureAwait(false);

                var batch = await db.Set<TRecord>()
                    .FromSql($"SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT {options.Value.BatchSize} FOR UPDATE SKIP LOCKED")
                    .ToListAsync(cancellationToken)
                    .ConfigureAwait(false);

                var published = 0;
                foreach (var record in batch)
                {
                    try
                    {
                        await producer.ProduceAsync(record.EventType, OutboxMessages.ToKafkaMessage(record), cancellationToken).ConfigureAwait(false);
                        record.PublishedAt = clock.GetUtcNow();
                        published++;
                    }
                    catch (ProduceException<string, string> ex)
                    {
                        record.Attempts++;
                        LogProduceFailed(logger, record.Id, record.Attempts, ex);
                        break;
                    }
                }

                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return published;
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Falló la publicación del Outbox.")]
    private static partial void LogPublishFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No se pudo publicar el evento {EventId} (intento {Attempt}).")]
    private static partial void LogProduceFailed(ILogger logger, Guid eventId, int attempt, Exception exception);
}