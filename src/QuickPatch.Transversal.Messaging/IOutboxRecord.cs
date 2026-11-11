namespace QuickPatch.Transversal.Messaging;

/// <summary>
/// Fila de <c>outbox_events</c> (DD 5.15) tal como la necesita el publicador. Cada servicio la implementa con su propia
/// clase mapeada en su DbContext, así el modelo de EF y las migraciones del servicio no cambian.
/// </summary>
public interface IOutboxRecord
{
    Guid Id { get; }

    Guid AggregateId { get; }

    string EventType { get; }

    string Payload { get; }

    DateTimeOffset? PublishedAt { get; set; }

    int Attempts { get; set; }
}