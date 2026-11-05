/**
 * Identifier helpers shared by the table DDL and the view DDL.
 *
 * They live apart from `ddl.ts` so that `views.ts` can use them without
 * importing the module that imports `views.ts` back for `buildSchemaSql`.
 */

/** MySQL identifiers are limited to 64 characters. */
export const MAX_IDENTIFIER_LENGTH = 64

/**
 * `name`, or a prefix of it with a hash suffix when it would not fit, so a
 * long stream or index name cannot produce one that MySQL refuses or that
 * collides with another's truncation.
 */
export function safeIdentifier(name: string): string {
  if (name.length <= MAX_IDENTIFIER_LENGTH) return name
  let hash = 5381
  for (let i = 0; i < name.length; i++) hash = ((hash << 5) + hash + name.charCodeAt(i)) >>> 0
  const suffix = `_${hash.toString(16).padStart(8, '0')}`
  return name.slice(0, MAX_IDENTIFIER_LENGTH - suffix.length) + suffix
}

/** Backtick-quoted, with embedded backticks doubled. */
export const quoteIdent = (name: string): string => `\`${name.replace(/`/g, '``')}\``

/** A single-quoted SQL string literal, with quotes and backslashes escaped. */
export const quoteLiteral = (value: string): string => `'${value.replace(/\\/g, '\\\\').replace(/'/g, "''")}'`
