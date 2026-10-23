/**
 * Identifier helpers shared by the table DDL and the view DDL.
 *
 * They live apart from `ddl.ts` so that `views.ts` can use them without
 * importing the module that imports `views.ts` back for `buildSchemaSql`.
 * `ddl.ts` re-exports them, so existing callers are unaffected.
 */

/** Postgres identifiers are limited to 63 bytes. */
export function safeIdentifier(name: string): string {
  if (Buffer.byteLength(name) <= 63) return name
  let hash = 5381
  for (let i = 0; i < name.length; i++) hash = ((hash << 5) + hash + name.charCodeAt(i)) >>> 0
  const suffix = `_${hash.toString(16).padStart(8, '0')}`
  return name.slice(0, 63 - suffix.length) + suffix
}

export const quoteIdent = (name: string): string => `"${name.replace(/"/g, '""')}"`
