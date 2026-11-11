using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using QuickPatch.Transversal.Http;

namespace QuickPatch.Transversal.Tests;

public sealed class CorrelationIdTests
{
    [Fact]
    public async Task UnUuidValido_SeConservaYSeDevuelve()
    {
        using var host = await StartAsync();
        var id = Guid.NewGuid();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/");
        request.Headers.Add(CorrelationId.HeaderName, id.ToString());

        var response = await host.GetTestClient().SendAsync(request);

        Assert.Equal(id.ToString(), response.Headers.GetValues(CorrelationId.HeaderName).Single());
        Assert.Equal(id.ToString(), await response.Content.ReadAsStringAsync());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("no-es-un-uuid")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public async Task SinUuidValido_SeGeneraUnoNuevo(string? header)
    {
        using var host = await StartAsync();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/");
        if (header is not null)
        {
            request.Headers.TryAddWithoutValidation(CorrelationId.HeaderName, header);
        }

        var response = await host.GetTestClient().SendAsync(request);

        var devuelto = Guid.Parse(response.Headers.GetValues(CorrelationId.HeaderName).Single());
        Assert.NotEqual(Guid.Empty, devuelto);
        Assert.Equal(devuelto.ToString(), await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public void FueraDelMiddleware_DevuelveVacio() =>
        Assert.Equal(Guid.Empty, CorrelationId.Get(new DefaultHttpContext()));

    private static async Task<IHost> StartAsync()
    {
        var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseTestServer()
                .ConfigureServices(services => services.AddLogging())
                .Configure(app =>
                {
                    app.UseCorrelationId();
                    app.Run(context => context.Response.WriteAsync(CorrelationId.Get(context).ToString()));
                }))
            .Build();
        await host.StartAsync();
        return host;
    }
}