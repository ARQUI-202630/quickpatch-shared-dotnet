using Microsoft.EntityFrameworkCore;

namespace QuickPatch.Transversal.Messaging;

/// <summary>Fila de <c>processed_events</c> (RN-EV1): un evento consumido que ya se aplicó.</summary>
public sealed class ProcessedEventRecord
{
    public Guid EventId { get; set; }

    public Guid TenantId { get; set; }

    public string EventType { get; set; } = string.Empty;

    public DateTimeOffset ProcessedAt { get; set; }
}

/// <summary>
/// Idempotencia de los consumidores (RN-EV1): consulta y registra eventos ya aplicados en <c>processed_events</c>, dentro
/// de la misma transacción del manejador. El servicio la expone a su capa Application con su propio puerto: su
/// <c>ProcessedEventRepository</c> hereda de esta clase e implementa ese puerto.
/// </summary>
/// <typeparam name="TContext">DbContext del servicio que mapea <c>processed_events</c>.</typeparam>
public class ProcessedEventStore<TContext>(TContext db)
    where TContext : DbContext
{
    public Task<bool> ExistsAsync(Guid eventId, CancellationToken cancellationToken) =>
        db.Set<ProcessedEventRecord>().AnyAsync(x => x.EventId == eventId, cancellationToken);

    public void Add(Guid eventId, Guid tenantId, string eventType, DateTimeOffset processedAt) =>
        db.Set<ProcessedEventRecord>().Add(new ProcessedEventRecord { EventId = eventId, TenantId = tenantId, EventType = eventType, ProcessedAt = processedAt });
}