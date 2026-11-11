using Microsoft.EntityFrameworkCore;

namespace QuickPatch.Transversal.Messaging;

/// <summary>
/// Mapeo de EF Core de las tablas transversales de mensajería, iguales en todas las bases .NET (DD 5.15 y RN-EV1). El
/// servicio lo llama desde su <c>OnModelCreating</c>; sus migraciones siguen siendo suyas.
/// </summary>
public static class TransversalModel
{
    /// <summary><c>outbox_events</c>, con el índice parcial de pendientes que lee el publicador.</summary>
    public static ModelBuilder MapOutboxEvents(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.Entity<OutboxEventRecord>(e =>
        {
            e.ToTable("outbox_events");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.TenantId).HasColumnName("tenant_id");
            e.Property(x => x.AggregateId).HasColumnName("aggregate_id");
            e.Property(x => x.EventType).HasColumnName("event_type").HasMaxLength(100);
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("jsonb");
            e.Property(x => x.CreatedAt).HasColumnName("created_at");
            e.Property(x => x.PublishedAt).HasColumnName("published_at");
            e.Property(x => x.Attempts).HasColumnName("attempts");
            e.HasIndex(x => x.CreatedAt).HasDatabaseName("ix_outbox_events_pending").HasFilter("published_at IS NULL");
        });
        return modelBuilder;
    }

    /// <summary><c>processed_events</c>: la clave es el <c>eventId</c> del sobre del evento.</summary>
    public static ModelBuilder MapProcessedEvents(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        modelBuilder.Entity<ProcessedEventRecord>(e =>
        {
            e.ToTable("processed_events");
            e.HasKey(x => x.EventId);
            e.Property(x => x.EventId).HasColumnName("event_id").ValueGeneratedNever();
            e.Property(x => x.TenantId).HasColumnName("tenant_id");
            e.Property(x => x.EventType).HasColumnName("event_type").HasMaxLength(100);
            e.Property(x => x.ProcessedAt).HasColumnName("processed_at");
        });
        return modelBuilder;
    }
}