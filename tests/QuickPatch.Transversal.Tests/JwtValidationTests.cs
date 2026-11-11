using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Security.Cryptography;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

using QuickPatch.Transversal.Security;

namespace QuickPatch.Transversal.Tests;

/// <summary>
/// La validación del JWT de Identity: acepta un token RS256 vigente del emisor y la audiencia correctos, y rechaza
/// cualquier otro (otra llave, otro emisor, vencido) y todo token si no hay llave (falla cerrado).
/// </summary>
public sealed class JwtValidationTests : IDisposable
{
    private readonly RSA rsa = RSA.Create(2048);

    public void Dispose() => rsa.Dispose();

    [Fact]
    public async Task UnTokenValido_EntraYLosClaimsLleganTalCual()
    {
        using var host = await HostAsync(rsa.ExportSubjectPublicKeyInfoPem());
        var (user, tenant) = (Guid.NewGuid(), Guid.NewGuid());

        var response = await Call(host, Token(rsa, user, tenant));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal($"{user}|{tenant}|cliente", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task OtraLlaveOtroEmisorOVencido_Responde401()
    {
        using var otra = RSA.Create(2048);
        using var host = await HostAsync(rsa.ExportSubjectPublicKeyInfoPem());

        Assert.Equal(HttpStatusCode.Unauthorized, (await Call(host, Token(otra, Guid.NewGuid(), Guid.NewGuid()))).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await Call(host, Token(rsa, Guid.NewGuid(), Guid.NewGuid(), issuer: "otro"))).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await Call(host, Token(rsa, Guid.NewGuid(), Guid.NewGuid(), expires: DateTime.UtcNow.AddMinutes(-5)))).StatusCode);
    }

    [Fact]
    public async Task SinLlave_NingunTokenEntra()
    {
        using var host = await HostAsync(string.Empty);

        Assert.Equal(HttpStatusCode.Unauthorized, (await Call(host, Token(rsa, Guid.NewGuid(), Guid.NewGuid()))).StatusCode);
        Assert.Null(JwtValidation.LoadPublicKey(" "));
    }

    [Fact]
    public void LosParametrosSoloAceptanRs256()
    {
        var parameters = JwtValidation.CreateParameters("emisor", "audiencia", null, TimeSpan.Zero);

        Assert.Equal(["RS256"], parameters.ValidAlgorithms);
        Assert.Equal(("emisor", "audiencia", "sub", "role", TimeSpan.Zero), (parameters.ValidIssuer, parameters.ValidAudience, parameters.NameClaimType, parameters.RoleClaimType, parameters.ClockSkew));
        var settings = new JwtSettings();
        Assert.Equal(("Jwt", "quickpatch-identity", "quickpatch", string.Empty), (JwtSettings.Section, settings.Issuer, settings.Audience, settings.PublicKeyPem));
    }

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    public void LosClaimsExigenUsuarioYTenant(bool conUsuario, bool conTenant, bool valido)
    {
        var claims = new List<Claim> { new(JwtClaims.Role, "tecnico") };
        if (conUsuario)
        {
            claims.Add(new Claim(JwtClaims.Subject, Guid.NewGuid().ToString()));
        }

        if (conTenant)
        {
            claims.Add(new Claim(JwtClaims.Tenant, Guid.NewGuid().ToString()));
        }

        var ok = JwtClaims.TryRead(new ClaimsPrincipal(new ClaimsIdentity(claims)), out _, out var tenant, out var role);

        Assert.Equal(valido, ok);
        Assert.Equal("tecnico", role);
        Assert.Equal(valido, tenant != Guid.Empty);
        Assert.False(JwtClaims.TryRead(new ClaimsPrincipal(new ClaimsIdentity([new Claim(JwtClaims.Subject, Guid.Empty.ToString())])), out _, out _, out _));
    }

    private static async Task<IHost> HostAsync(string publicKeyPem)
    {
        var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddQuickPatchJwtBearer(new JwtSettings { PublicKeyPem = publicKeyPem }, TimeSpan.Zero);
                    services.AddAuthorization();
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseAuthentication();
                    app.UseAuthorization();
                    app.UseEndpoints(e => e.MapGet("/yo", (ClaimsPrincipal p) =>
                        JwtClaims.TryRead(p, out var u, out var t, out var r) ? Results.Text($"{u}|{t}|{r}") : Results.BadRequest())
                        .RequireAuthorization());
                }))
            .Build();
        await host.StartAsync();
        return host;
    }

    private static Task<HttpResponseMessage> Call(IHost host, string token)
    {
        var client = host.GetTestClient();
        var request = new HttpRequestMessage(HttpMethod.Get, "/yo");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        return client.SendAsync(request);
    }

    private static string Token(RSA key, Guid user, Guid tenant, string issuer = "quickpatch-identity", DateTime? expires = null)
    {
        var ahora = DateTime.UtcNow;
        var vence = expires ?? ahora.AddMinutes(5);
        return new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = "quickpatch",
            NotBefore = vence.AddMinutes(-10),
            IssuedAt = vence.AddMinutes(-10),
            Expires = vence,
            Claims = new Dictionary<string, object> { ["sub"] = user.ToString(), ["tenant_id"] = tenant.ToString(), ["role"] = "cliente" },
            SigningCredentials = new SigningCredentials(new RsaSecurityKey(key), SecurityAlgorithms.RsaSha256),
        });
    }
}