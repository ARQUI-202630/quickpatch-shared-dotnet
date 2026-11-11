using Microsoft.EntityFrameworkCore;

namespace QuickPatch.Transversal.Persistence;

/// <summary>
/// Transacción por tenant (DD 10.2, ADR-005): abre una transacción, fija <c>app.current_tenant</c> con
/// <c>set_config(..., true)</c> (equivalente a <c>SET LOCAL</c>), ejecuta el trabajo, guarda y confirma. RLS filtra todo
/// lo que se lea o escriba dentro. Si algo falla, la transacción se revierte y la excepción sale tal cual, para que el
/// adaptador del servicio la traduzca a sus excepciones de dominio (por ejemplo, un índice único violado).
/// </summary>
public static class TenantTransaction
{
    /// <summary>Variable de sesión que leen las políticas RLS de todas las bases (DD 10.2).</summary>
    public const string SettingName = "app.current_tenant";

    public static async Task<T> ExecuteAsync<T>(DbContext db, Guid tenantId, Func<CancellationToken, Task<T>> work, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(work);
        if (tenantId == Guid.Empty)
        {
            throw new ArgumentException("El tenant es obligatorio.", nameof(tenantId));
        }

        var transaction = await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (transaction.ConfigureAwait(false))
        {
            await SetTenantAsync(db, tenantId, cancellationToken).ConfigureAwait(false);

            var result = await work(cancellationToken).ConfigureAwait(false);

            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
    }

    /// <summary>Fija el tenant en la transacción en curso; se usa cuando el servicio ya abrió la transacción.</summary>
    public static async Task SetTenantAsync(DbContext db, Guid tenantId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(db);
        if (tenantId == Guid.Empty)
        {
            throw new ArgumentException("El tenant es obligatorio.", nameof(tenantId));
        }

        var tenant = tenantId.ToString();
        await db.Database.ExecuteSqlAsync($"SELECT set_config('app.current_tenant', {tenant}, true)", cancellationToken).ConfigureAwait(false);
    }
}