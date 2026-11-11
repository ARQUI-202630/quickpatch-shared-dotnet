using System.Text.RegularExpressions;

using Microsoft.EntityFrameworkCore;

namespace QuickPatch.Transversal.Persistence;

/// <summary>
/// Adopta un rol de la base dentro de la transacción en curso (<c>SET LOCAL ROLE</c>, DD 10.2). Vacío no cambia de rol
/// (desarrollo local con el dueño de las tablas).
/// </summary>
public static partial class SessionRole
{
    public static async Task SetAsync(DbContext db, string role, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(db);
        if (string.IsNullOrWhiteSpace(role))
        {
            return;
        }

        if (!IsValidName(role))
        {
            throw new InvalidOperationException("El nombre del rol de la base no es válido.");
        }

#pragma warning disable EF1002 // El nombre del rol se validó con una expresión regular estricta; no admite parámetros.
        await db.Database.ExecuteSqlRawAsync($"SET LOCAL ROLE \"{role}\"", cancellationToken).ConfigureAwait(false);
#pragma warning restore EF1002
    }

    /// <summary>Nombre de rol de PostgreSQL sin comillas: minúsculas, dígitos y guion bajo, hasta 63 caracteres.</summary>
    public static bool IsValidName(string role) => role is not null && RoleName().IsMatch(role);

    [GeneratedRegex("^[a-z_][a-z0-9_]{0,62}$")]
    private static partial Regex RoleName();
}