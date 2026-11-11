# quickpatch-shared-dotnet

Código transversal de los servicios .NET de QUICKPATCH (ADR-020, escenario AC7-E5). Hasta el 11 de noviembre de 2026 este código estaba copiado en cada servicio, y las copias ya divergían: ServiceRequest había corregido el apagado del publicador del Outbox y los demás no.

## Qué hay

Un proyecto por mecanismo. Cada servicio referencia solo los que usa.

| Proyecto | Contenido | Dependencias |
|---|---|---|
| `QuickPatch.Transversal.Http` | `CorrelationId`: middleware de `X-Correlation-Id` y su lectura (contrato OpenAPI) | ASP.NET Core |
| `QuickPatch.Transversal.Persistence` | `SessionRole`: `SET LOCAL ROLE` con el nombre validado (DD 10.2) | EF Core relacional |
| `QuickPatch.Transversal.Messaging` | `OutboxPublisher<TContext, TRecord>`, `OutboxOptions`, `IOutboxRecord` y `OutboxMessages` (ADR-007) | Persistence, Confluent.Kafka, EF Core relacional |

## Reglas (ADR-020)

1. **Sin dominio.** Aquí no hay entidades, eventos, contratos ni reglas de negocio. Cada servicio conserva su tabla `outbox_events`, la clase que la mapea (implementa `IOutboxRecord`), su DbContext, sus migraciones y sus contratos de `quickpatch-kafka`.
2. **Sin clase gigante.** Un proyecto por mecanismo, cada uno pequeño. Lo que solo usa un servicio se queda en ese servicio.
3. **Versión fijada por el servicio.** Cada servicio consume este repositorio como submódulo en `shared/dotnet`, fijado a una etiqueta `vX.Y.Z`. Actualizar es un PR propio del servicio, con su CI y su despliegue en QA.
4. **Compatibilidad.** Un cambio incompatible sube la versión mayor y se anuncia a los dueños de los servicios. Cambiar el comportamiento sin cambiar la firma (por ejemplo, el reintento del publicador) también es un cambio que se anota en las notas de la etiqueta.
5. **Matching (Java)** aplica las mismas convenciones en su stack; la equivalencia se verifica con los contratos y las pruebas, no con código compartido.

## Cómo lo consume un servicio

```bash
git submodule add https://github.com/ARQUI-202630/quickpatch-shared-dotnet.git shared/dotnet
git -C shared/dotnet checkout v0.1.0
```

En el `.csproj` de la capa que lo usa (Api para Http; Infrastructure para Persistence y Messaging):

```xml
<ProjectReference Include="..\..\shared\dotnet\src\QuickPatch.Transversal.Messaging\QuickPatch.Transversal.Messaging.csproj" />
```

En el `Dockerfile`, junto a `COPY src/ src/`: `COPY shared/ shared/`. El CI ya hace `checkout` con `submodules: recursive`.

Registro del publicador en Infrastructure:

```csharp
services.Configure<OutboxOptions>(configuration.GetSection(OutboxOptions.Section));
services.AddSingleton<OutboxPublisher<CatalogDbContext, OutboxEventRecord>>();
services.AddHostedService(sp => sp.GetRequiredService<OutboxPublisher<CatalogDbContext, OutboxEventRecord>>());
```

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
