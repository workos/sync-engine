/**
 * Identifier and literal helpers shared by the table DDL and the view DDL.
 *
 * They live apart from `ddl.ts` so that `views.ts` can use them without
 * importing the module that imports `views.ts` back for `buildSchemaSql`.
 */

/**
 * ClickHouse has no hard identifier limit, but the stream names become
 * Keeper paths and file names on disk; the SQL packages' 63-byte habit is
 * kept so one catalog produces one set of names everywhere.
 */
export const MAX_IDENTIFIER_LENGTH = 63

export function safeIdentifier(name: string): string {
  if (name.length <= MAX_IDENTIFIER_LENGTH) return name
  let hash = 5381
  for (let i = 0; i < name.length; i++) hash = ((hash << 5) + hash + name.charCodeAt(i)) >>> 0
  const suffix = `_${hash.toString(16).padStart(8, '0')}`
  return name.slice(0, MAX_IDENTIFIER_LENGTH - suffix.length) + suffix
}

/** Backtick-quoted, with embedded backticks and backslashes escaped. */
export const quoteIdent = (name: string): string => `\`${name.replace(/\\/g, '\\\\').replace(/`/g, '\\`')}\``

/** A single-quoted SQL string literal, with backslashes and quotes escaped the way ClickHouse reads them. */
export const quoteLiteral = (value: string): string => `'${value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`
