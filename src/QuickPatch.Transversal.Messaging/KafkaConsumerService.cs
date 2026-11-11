using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace QuickPatch.Transversal.Messaging;

/// <summary>
/// Bucle de consumo de Kafka común a los servicios .NET (ADR-006, ADR-027): entrega al menos una vez. Se suscribe a los
/// topics, confirma el offset solo cuando <see cref="ProcessAsync"/> termina bien y, si devuelve <c>false</c> o falla,
/// vuelve al mismo mensaje y reintenta. Los errores de lectura no fatales (por ejemplo, el topic todavía no existe, hallazgo
/// de POC-BE-001) se registran y se reintentan sin detener el servicio. Qué hacer con cada mensaje (contrato, manejador,
/// idempotencia) es del servicio, que hereda de esta clase.
/// </summary>
public abstract partial class KafkaConsumerService : BackgroundService
{
    private readonly IConsumer<string, string> consumer;
    private readonly string[] topics;
    private readonly ILogger logger;
    private readonly TimeSpan readRetryDelay;
    private readonly TimeSpan processRetryDelay;

    /// <param name="consumer">Consumidor con <c>EnableAutoCommit = false</c>.</param>
    /// <param name="topics">Topics, uno por tipo de evento (ADR-027).</param>
    /// <param name="logger">Registro del servicio.</param>
    /// <param name="readRetryDelay">Espera tras un error de lectura no fatal; 5 s si no se indica.</param>
    /// <param name="processRetryDelay">Espera antes de reintentar un mensaje que no se pudo aplicar; 2 s si no se indica.</param>
    protected KafkaConsumerService(
        IConsumer<string, string> consumer,
        IEnumerable<string> topics,
        ILogger logger,
        TimeSpan? readRetryDelay = null,
        TimeSpan? processRetryDelay = null)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(topics);
        ArgumentNullException.ThrowIfNull(logger);
        this.consumer = consumer;
        this.topics = [.. topics];
        this.logger = logger;
        this.readRetryDelay = readRetryDelay ?? TimeSpan.FromSeconds(5);
        this.processRetryDelay = processRetryDelay ?? TimeSpan.FromSeconds(2);
    }

    /// <summary>Nombre del consumidor en el registro; por defecto, el de la clase.</summary>
    protected virtual string Name => GetType().Name;

    /// <summary>Aplica un mensaje; devuelve <c>false</c> si hay que reintentarlo.</summary>
    protected abstract Task<bool> ProcessAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken);

    // Consume es bloqueante: el bucle corre en un hilo propio para no ocupar el del arranque del host.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(() => ConsumeLoopAsync(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        consumer.Subscribe(topics);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    LogConsumeFailed(logger, Name, ex.Error.Reason, ex);
                    await Task.Delay(readRetryDelay, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                if (result?.Message?.Value is null)
                {
                    continue;
                }

                if (await TryProcessAsync(result, stoppingToken).ConfigureAwait(false))
                {
                    consumer.Commit(result);
                }
                else
                {
                    consumer.Seek(result.TopicPartitionOffset);
                    await Task.Delay(processRetryDelay, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task<bool> TryProcessAsync(ConsumeResult<string, string> result, CancellationToken stoppingToken)
    {
        try
        {
            return await ProcessAsync(result, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            LogProcessingFailed(logger, Name, result.Topic, result.Offset.Value, ex);
            return false;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Consumer}: no se pudo leer de Kafka ({Reason}); se reintentará.")]
    private static partial void LogConsumeFailed(ILogger logger, string consumer, string reason, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Consumer}: falló el procesamiento de {Topic} offset {Offset}; se reintentará.")]
    private static partial void LogProcessingFailed(ILogger logger, string consumer, string topic, long offset, Exception exception);
}