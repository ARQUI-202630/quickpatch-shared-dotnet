using System.Collections.Concurrent;
using System.Reflection;

using Confluent.Kafka;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;

using QuickPatch.Transversal.Http;
using QuickPatch.Transversal.Messaging;

namespace QuickPatch.Transversal.Tests;

/// <summary>
/// El bucle de consumo: confirma solo lo aplicado, vuelve al mismo mensaje si falla o pide reintento, sobrevive a los
/// errores de lectura no fatales y cierra el consumidor al detenerse.
/// </summary>
public sealed class KafkaConsumerServiceTests
{
    [Fact]
    public async Task ConfirmaElMensajeAplicado()
    {
        var consumer = FakeConsumer.Create(Mensaje("a", 1));
        var service = new TestConsumer(consumer, _ => Task.FromResult(true));

        await RunUntilAsync(service, () => FakeConsumer.Of(consumer).Committed.Count == 1);

        Assert.Equal(1, FakeConsumer.Of(consumer).Committed.Single().Offset.Value);
        Assert.Equal(["x.created"], FakeConsumer.Of(consumer).Subscribed);
        Assert.Empty(FakeConsumer.Of(consumer).Seeks);
        Assert.True(FakeConsumer.Of(consumer).Closed);
    }

    [Fact]
    public async Task SiPideReintento_VuelveAlMismoMensajeSinConfirmar()
    {
        var consumer = FakeConsumer.Create(Mensaje("a", 7));
        var service = new TestConsumer(consumer, _ => Task.FromResult(false));

        await RunUntilAsync(service, () => !FakeConsumer.Of(consumer).Seeks.IsEmpty);

        Assert.Equal(7, FakeConsumer.Of(consumer).Seeks.First().Offset.Value);
        Assert.Empty(FakeConsumer.Of(consumer).Committed);
    }

    [Fact]
    public async Task SiElManejadorFalla_ReintentaSinConfirmar()
    {
        var consumer = FakeConsumer.Create(Mensaje("a", 3));
        var service = new TestConsumer(consumer, _ => throw new InvalidOperationException("base caída"));

        await RunUntilAsync(service, () => !FakeConsumer.Of(consumer).Seeks.IsEmpty);

        Assert.Empty(FakeConsumer.Of(consumer).Committed);
    }

    [Fact]
    public async Task UnErrorDeLecturaNoFatal_SeReintentaYSigue()
    {
        var consumer = FakeConsumer.Create(new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.UnknownTopicOrPart)), Mensaje("a", 2));
        var service = new TestConsumer(consumer, _ => Task.FromResult(true));

        await RunUntilAsync(service, () => FakeConsumer.Of(consumer).Committed.Count == 1);

        Assert.Equal(2, FakeConsumer.Of(consumer).Committed.Single().Offset.Value);
    }

    [Fact]
    public async Task UnMensajeSinValor_SeSalta()
    {
        var consumer = FakeConsumer.Create(Mensaje(null, 1), Mensaje("b", 2));
        var vistos = new ConcurrentQueue<string>();
        var service = new TestConsumer(consumer, r =>
        {
            vistos.Enqueue(r.Message.Value);
            return Task.FromResult(true);
        });

        await RunUntilAsync(service, () => FakeConsumer.Of(consumer).Committed.Count == 1);

        Assert.Equal(["b"], vistos);
    }

    [Fact]
    public void ElRegistroEstructuradoUsaJsonConScopes()
    {
        using var provider = new ServiceCollection().AddLogging(l => l.AddQuickPatchJsonConsole()).BuildServiceProvider();

        var options = provider.GetRequiredService<IOptionsMonitor<JsonConsoleFormatterOptions>>().CurrentValue;
        Assert.True(options.IncludeScopes);
        Assert.Equal(ConsoleFormatterNames.Json, provider.GetRequiredService<IOptionsMonitor<ConsoleLoggerOptions>>().CurrentValue.FormatterName);
        Assert.Throws<ArgumentNullException>(() => StructuredLogging.AddQuickPatchJsonConsole(null!));
    }

    private static ConsumeResult<string, string> Mensaje(string? value, long offset) => new ConsumeResult<string, string>
    {
        Message = new Message<string, string> { Key = "k", Value = value! },
        TopicPartitionOffset = new TopicPartitionOffset("x.created", 0, offset),
    };

    private static async Task RunUntilAsync(TestConsumer service, Func<bool> done)
    {
        await service.StartAsync(CancellationToken.None);
        for (var i = 0; i < 100 && !done(); i++)
        {
            await Task.Delay(20);
        }

        await service.StopAsync(CancellationToken.None);
        Assert.True(done());
    }

    private sealed class TestConsumer(IConsumer<string, string> consumer, Func<ConsumeResult<string, string>, Task<bool>> process)
        : KafkaConsumerService(consumer, ["x.created"], NullLogger.Instance, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10))
    {
        protected override string Name => "prueba";

        protected override Task<bool> ProcessAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken) => process(result);
    }
}

/// <summary>Consumidor de Kafka en memoria: entrega en orden los mensajes o excepciones indicados y después nada.</summary>
public class FakeConsumer : DispatchProxy
{
    private readonly ConcurrentQueue<object> pending = new();

    public List<string> Subscribed { get; } = [];

    public ConcurrentQueue<TopicPartitionOffset> Committed { get; } = new();

    public ConcurrentQueue<TopicPartitionOffset> Seeks { get; } = new();

    public bool Closed { get; private set; }

    public static IConsumer<string, string> Create(params object[] items)
    {
        var consumer = DispatchProxy.Create<IConsumer<string, string>, FakeConsumer>();
        foreach (var item in items)
        {
            Of(consumer).pending.Enqueue(item);
        }

        return consumer;
    }

    public static FakeConsumer Of(IConsumer<string, string> consumer) => (FakeConsumer)(object)consumer;

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod);
        switch (targetMethod.Name)
        {
            case nameof(IConsumer<string, string>.Subscribe) when args is [IEnumerable<string> topics, ..]:
                Subscribed.AddRange(topics);
                return null;
            case nameof(IConsumer<string, string>.Consume):
                if (pending.TryDequeue(out var item))
                {
                    return item is Exception ex ? throw ex : item;
                }

                Thread.Sleep(5);
                return null;
            case nameof(IConsumer<string, string>.Commit) when args is [ConsumeResult<string, string> result, ..]:
                Committed.Enqueue(result.TopicPartitionOffset);
                return null;
            case nameof(IConsumer<string, string>.Seek) when args is [TopicPartitionOffset offset, ..]:
                // El mensaje no se vuelve a entregar: basta con saber que se pidió volver a él.
                Seeks.Enqueue(offset);
                return null;
            case nameof(IConsumer<string, string>.Close):
                Closed = true;
                return null;
            case "Dispose":
                return null;
            default:
                throw new NotSupportedException(targetMethod.Name);
        }
    }
}