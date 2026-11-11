using System.Security.Claims;
using System.Security.Cryptography;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;

namespace QuickPatch.Transversal.Security;

/// <summary>Configuración del JWT (sección <c>Jwt</c>). Identity firma con RS256; los demás servicios solo tienen la llave pública.</summary>
public sealed class JwtSettings
{
    public const string Section = "Jwt";

    public string Issuer { get; set; } = "quickpatch-identity";

    public string Audience { get; set; } = "quickpatch";

    /// <summary>Llave pública RSA en PEM. Sin llave, ningún token es válido (falla cerrado).</summary>
    public string PublicKeyPem { get; set; } = string.Empty;
}

/// <summary>Claims del JWT de Identity (ADR-018). El tenant sale siempre del token, nunca del cuerpo ni de cabeceras (RN-U3).</summary>
public static class JwtClaims
{
    public const string Subject = "sub";
    public const string Tenant = "tenant_id";
    public const string Role = "role";
    public const string Company = "company_id";

    /// <summary>Lee usuario, tenant y rol; falla si el usuario o el tenant faltan o están vacíos.</summary>
    public static bool TryRead(ClaimsPrincipal principal, out Guid userId, out Guid tenantId, out string role)
    {
        ArgumentNullException.ThrowIfNull(principal);
        role = principal.FindFirstValue(Role) ?? string.Empty;
        tenantId = Guid.Empty;
        return Guid.TryParse(principal.FindFirstValue(Subject), out userId) && userId != Guid.Empty
            && Guid.TryParse(principal.FindFirstValue(Tenant), out tenantId) && tenantId != Guid.Empty;
    }
}

/// <summary>
/// Validación del JWT RS256 de Identity (ADR-018), igual en todos los servicios .NET: firma con la llave pública, solo
/// RS256, emisor, audiencia y vigencia. Sin llave, falla cerrado: toda petición protegida responde 401.
/// </summary>
public static class JwtValidation
{
    /// <summary>Llave pública desde PEM; <c>null</c> si no hay (falla cerrado).</summary>
    public static RsaSecurityKey? LoadPublicKey(string pem)
    {
        if (string.IsNullOrWhiteSpace(pem))
        {
            return null;
        }

        // La llave vive lo que vive la aplicación: Microsoft.IdentityModel guarda en caché el proveedor de firma por la
        // huella de la llave, y liberar el RSA dejaría en la caché un proveedor inservible.
#pragma warning disable CA2000
        var rsa = RSA.Create();
#pragma warning restore CA2000
        rsa.ImportFromPem(pem);
        return new RsaSecurityKey(rsa);
    }

    /// <summary>Parámetros de validación comunes, con la tolerancia de reloj que pide cada uso.</summary>
    public static TokenValidationParameters CreateParameters(string issuer, string audience, SecurityKey? signingKey, TimeSpan clockSkew) => new()
    {
        ValidIssuer = issuer,
        ValidAudience = audience,
        IssuerSigningKey = signingKey,
        ValidAlgorithms = [SecurityAlgorithms.RsaSha256],
        NameClaimType = JwtClaims.Subject,
        RoleClaimType = JwtClaims.Role,
        ClockSkew = clockSkew,
    };

    /// <summary>Bearer de ASP.NET Core con los claims tal como vienen en el token (sin mapear a los de Microsoft).</summary>
    public static AuthenticationBuilder AddQuickPatchJwtBearer(this IServiceCollection services, JwtSettings settings, TimeSpan clockSkew)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(settings);
        var parameters = CreateParameters(settings.Issuer, settings.Audience, LoadPublicKey(settings.PublicKeyPem), clockSkew);
        return services
            .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                options.MapInboundClaims = false;
                options.TokenValidationParameters = parameters;
            });
    }
}