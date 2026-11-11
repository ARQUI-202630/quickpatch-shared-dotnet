namespace QuickPatch.Transversal.Messaging;

/// <summary>
/// Fila de <c>outbox_events</c> (DD 5.15), igual en todas las bases de los servicios .NET. Cada servicio la mapea en su
/// propio DbContext con <see cref="TransversalModel.MapOutboxEvents"/> y conserva su tabla y sus migraciones.
/// </summary>
public sealed class OutboxEventRecord : IOutboxRecord
{
    public Guid Id { get; set; }

    public Guid TenantId { get; set; }

    public Guid AggregateId { get; set; }

    public string EventType { get; set; } = string.Empty;

    public string Payload { get; set; } = string.Empty;

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset? PublishedAt { get; set; }

    public int Attempts { get; set; }

    /// <summary>Evento nuevo, pendiente de publicar, para guardarlo en la transacción del caso de uso (ADR-007).</summary>
    public static OutboxEventRecord Pending(Guid id, Guid tenantId, Guid aggregateId, string eventType, string payload, DateTimeOffset createdAt) => new()
    {
        Id = id,
        TenantId = tenantId,
        AggregateId = aggregateId,
        EventType = eventType,
        Payload = payload,
        CreatedAt = createdAt,
        Attempts = 0,
    };
}