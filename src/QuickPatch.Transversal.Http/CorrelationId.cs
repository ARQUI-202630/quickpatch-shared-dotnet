using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace QuickPatch.Transversal.Http;

/// <summary>
/// Cabecera <c>X-Correlation-Id</c> (contrato OpenAPI): si llega un UUID válido se usa; si no, se genera uno.
/// Se devuelve en la respuesta, se agrega a los logs y viaja en los eventos (<c>correlationId</c>).
/// </summary>
public static class CorrelationId
{
    public const string HeaderName = "X-Correlation-Id";
    private const string ItemKey = "QuickPatch.CorrelationId";

    public static Guid Get(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.Items.TryGetValue(ItemKey, out var value) && value is Guid id ? id : Guid.Empty;
    }

    public static IApplicationBuilder UseCorrelationId(this IApplicationBuilder app) =>
        app.Use(async (context, next) =>
        {
            var header = context.Request.Headers[HeaderName].ToString();
            var id = Guid.TryParse(header, out var parsed) && parsed != Guid.Empty ? parsed : Guid.NewGuid();
            context.Items[ItemKey] = id;
            context.Response.Headers[HeaderName] = id.ToString();

            var logger = context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("QuickPatch.Http");
            using (logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = id }))
            {
                await next(context).ConfigureAwait(false);
            }
        });
}