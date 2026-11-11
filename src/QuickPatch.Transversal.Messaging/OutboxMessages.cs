using System.Text;

using Confluent.Kafka;

namespace QuickPatch.Transversal.Messaging;

/// <summary>Traducción de una fila de <c>outbox_events</c> al mensaje de Kafka (ADR-007).</summary>
public static class OutboxMessages
{
    /// <summary>Clave del agregado, el payload tal cual y las cabeceras <c>eventId</c> y <c>eventType</c>.</summary>
    public static Message<string, string> ToKafkaMessage(IOutboxRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return new Message<string, string>
        {
            Key = record.AggregateId.ToString(),
            Value = record.Payload,
            Headers = new Headers
            {
                { "eventId", Encoding.UTF8.GetBytes(record.Id.ToString()) },
                { "eventType", Encoding.UTF8.GetBytes(record.EventType) },
            },
        };
    }
}