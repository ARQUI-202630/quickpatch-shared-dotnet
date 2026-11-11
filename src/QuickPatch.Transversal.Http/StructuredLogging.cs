using Microsoft.Extensions.Logging;

namespace QuickPatch.Transversal.Http;

/// <summary>
/// Registro estructurado común (ADR-020): JSON en la consola, con los scopes. Así cada línea lleva el campo
/// <c>correlationId</c> del scope que abre <see cref="CorrelationId.UseCorrelationId"/>, y el recolector de la plataforma la puede
/// filtrar por ese campo.
/// </summary>
public static class StructuredLogging
{
    public static ILoggingBuilder AddQuickPatchJsonConsole(this ILoggingBuilder logging)
    {
        ArgumentNullException.ThrowIfNull(logging);
        return logging.AddJsonConsole(options => options.IncludeScopes = true);
    }
}