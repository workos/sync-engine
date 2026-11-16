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

/**
 * Where the mirror lives: a database, and a prefix on every object the
 * engine names in it.
 *
 * The prefix is for a database the mirror has to share. On plain MySQL the
 * mirror gets a database to itself and needs none. A PlanetScale branch is
 * one database, the application's tables are in it, and most applications
 * already have a `users`: without a prefix `init` takes that table for its
 * own and fails adding columns to it. The two travel together, in the place
 * the database name alone used to be passed, so that no statement can name
 * the database and forget the prefix.
 */
export interface Target {
  schema: string
  prefix: string
}

/** A bare database name means no prefix. */
export type SchemaRef = string | Target

export const target = (ref: SchemaRef): Target => (typeof ref === 'string' ? { schema: ref, prefix: '' } : ref)

/** `name` as the engine creates it: prefixed, then shortened to fit like any identifier. */
export const objectName = (ref: SchemaRef, name: string): string => safeIdentifier(`${target(ref).prefix}${name}`)

/** `` `schema`.`prefixname` ``: how every statement names a table or a view. */
export const qualified = (ref: SchemaRef, name: string): string =>
  `${quoteIdent(target(ref).schema)}.${quoteIdent(objectName(ref, name))}`

/**
 * The longest prefix accepted. It leaves every name the catalog generates,
 * `directory_group_memberships_live` being the longest, whole within the 64
 * characters, so a prefixed name is still the one a reader would guess.
 */
export const MAX_PREFIX_LENGTH = 24

const PREFIX = new RegExp(`^[a-z0-9_]{1,${MAX_PREFIX_LENGTH}}$`)

/**
 * The table prefix as an option, a flag or a variable gave it, checked.
 * Absent, empty or blank means none: a variable set to nothing is how a
 * deployment template says "no prefix". Anything else is lowercase letters,
 * digits and underscores, because it is written into the name of every
 * table and has to mean the same thing quoted or not. Lowercase only,
 * because a server with `lower_case_table_names` set stores names in
 * lowercase: a mixed-case prefix would be created under one spelling and
 * looked for in `information_schema` under another.
 */
export function parseTablePrefix(value: string | undefined): string {
  const prefix = (value ?? '').trim()
  if (prefix === '') return ''
  if (!PREFIX.test(prefix)) {
    throw new Error(
      `invalid table prefix ${JSON.stringify(value)}: expected lowercase letters, digits and underscores, at most ${MAX_PREFIX_LENGTH} characters`,
    )
  }
  return prefix
}
