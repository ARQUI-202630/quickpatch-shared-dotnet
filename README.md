# quickpatch-shared-dotnet

Código transversal de los servicios .NET de QUICKPATCH (ADR-020, escenario AC7-E5). Hasta el 11 de noviembre de 2026 este código estaba copiado en cada servicio, y las copias ya divergían: ServiceRequest había corregido el apagado del publicador del Outbox y los demás no.

## Qué hay

Un proyecto por mecanismo. Cada servicio referencia solo los que usa.

| Proyecto | Contenido | Capa que lo usa | Dependencias |
|---|---|---|---|
| `QuickPatch.Transversal.Http` | `CorrelationId`: middleware de `X-Correlation-Id` y su lectura (contrato OpenAPI). `StructuredLogging.AddQuickPatchJsonConsole`: registro en JSON con scopes, cada línea con su `correlationId` | Api | ASP.NET Core |
| `QuickPatch.Transversal.Persistence` | `TenantTransaction`: transacción con `set_config('app.current_tenant', ...)` para RLS (DD 10.2, ADR-005). `SessionRole`: `SET LOCAL ROLE` con el nombre validado | Infrastructure | EF Core relacional |
| `QuickPatch.Transversal.Messaging` | Outbox (ADR-007): `OutboxEventRecord`, `OutboxPublisher<TContext, TRecord>`, `OutboxOptions` y `OutboxMessages`. Idempotencia (RN-EV1): `ProcessedEventRecord` y `ProcessedEventStore<TContext>`. `TransversalModel`: mapeo de `outbox_events` y `processed_events`. `KafkaConsumerService`: bucle de consumo que entrega al menos una vez (ADR-006, ADR-027) | Infrastructure | Persistence, Confluent.Kafka, EF Core relacional |

## Reglas (ADR-020)

1. **Sin dominio.** Aquí no hay entidades, eventos, contratos ni reglas de negocio. Cada servicio conserva su base, sus migraciones, su DbContext, sus contratos de `quickpatch-kafka` y la traducción de errores a sus excepciones de dominio.
2. **Las capas del servicio no cambian.** Domain y Application no referencian este repositorio. Los puertos (`ITenantUnitOfWork`, `IOutbox`, `IProcessedEventRepository`) siguen en la capa Application de cada servicio. Infrastructure los implementa con adaptadores delgados que delegan aquí, y Api usa `QuickPatch.Transversal.Http`.
3. **Sin clase gigante.** Un proyecto por mecanismo, cada uno pequeño. Lo que solo usa un servicio se queda en ese servicio.
4. **Versión fijada por el servicio.** Cada servicio consume este repositorio como submódulo en `shared/dotnet`, fijado a una etiqueta `vX.Y.Z`. Actualizar es un PR propio del servicio, con su CI y su despliegue en QA.
5. **Compatibilidad.** Un cambio incompatible sube la versión mayor y se anuncia a los dueños de los servicios. Cambiar el comportamiento sin cambiar la firma (por ejemplo, el reintento del publicador) también es un cambio que se anota en las notas de la etiqueta.
6. **Matching (Java)** aplica las mismas convenciones en su stack; la equivalencia se verifica con los contratos y las pruebas, no con código compartido.

## Cómo lo consume un servicio

```bash
git submodule add https://github.com/ARQUI-202630/quickpatch-shared-dotnet.git shared/dotnet
git -C shared/dotnet checkout v0.2.0
```

En el `.csproj` de la capa que lo usa (Api para Http; Infrastructure para Persistence y Messaging):

```xml
<ProjectReference Include="..\..\shared\dotnet\src\QuickPatch.Transversal.Messaging\QuickPatch.Transversal.Messaging.csproj" />
```

En el `Dockerfile`, junto a `COPY src/ src/`: `COPY shared/ shared/`. El CI ya hace `checkout` con `submodules: recursive`.

Uso en Infrastructure:

```csharp
// DbContext: las tablas transversales con el mapeo común.
modelBuilder.MapOutboxEvents().MapProcessedEvents();

// Puerto de Application implementado con la transacción común; la traducción de errores es del servicio.
public sealed class TenantUnitOfWork(CatalogDbContext db) : ITenantUnitOfWork
{
    public Task<T> ExecuteAsync<T>(Guid tenantId, Func<CancellationToken, Task<T>> work, CancellationToken ct) =>
        TenantTransaction.ExecuteAsync(db, tenantId, work, ct);
}

// Idempotencia: el repositorio del servicio hereda la implementación común.
public sealed class ProcessedEventRepository(PaymentsDbContext db) : ProcessedEventStore<PaymentsDbContext>(db), IProcessedEventRepository;

// Consumidor: el servicio solo decide qué hacer con cada mensaje.
public sealed class ServiceRequestRatedConsumer(...) : KafkaConsumerService(consumer, [EventTypes.ServiceRequestRated], logger)
{
    protected override Task<bool> ProcessAsync(ConsumeResult<string, string> result, CancellationToken ct) => ...;
}

// Publicador del Outbox.
services.Configure<OutboxOptions>(configuration.GetSection(OutboxOptions.Section));
services.AddSingleton<OutboxPublisher<CatalogDbContext, OutboxEventRecord>>();
services.AddHostedService(sp => sp.GetRequiredService<OutboxPublisher<CatalogDbContext, OutboxEventRecord>>());
```

En Api: `builder.Logging.AddQuickPatchJsonConsole();` y `app.UseCorrelationId();`.

## Versiones

- **v0.2.0**: transacción por tenant, idempotencia, mapeo común de `outbox_events` y `processed_events`, bucle consumidor de Kafka y registro estructurado. Compatible con v0.1.0.
- **v0.1.0**: correlación, rol de sesión y publicador del Outbox.

## Desarrollo

```bash
dotnet restore
dotnet format --verify-no-changes --no-restore
dotnet build --no-restore -c Release
dotnet test tests/QuickPatch.Transversal.Tests --no-build -c Release   # necesita Docker (Testcontainers)
```

El CI exige formato, compilación sin advertencias, pruebas y cobertura de líneas ≥ 80 %.

## Trazabilidad

ADR-020 (aceptado el 11 de noviembre de 2026, SCRUM-657), ADR-007 (Outbox), ADR-013 (excepción: librería transversal sin dominio), ADR-021 (repositorio 13 de componentes) y escenario AC7-E5.
