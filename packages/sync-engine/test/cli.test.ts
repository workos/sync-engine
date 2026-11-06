/**
 * The command line's shared core: how flags and the environment are read,
 * the help text rendered from a spec, and the commands that need no
 * database. Each destination's own tests run the whole command against its
 * database.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { Destination } from '../src/destination.js'
import {
  invokedDirectly,
  parseArgs,
  parseExcludeStreams,
  parseHydrate,
  parseLeaseTtl,
  parseMaxPages,
  parseSslCa,
  repairRequested,
  streamsFromFlags,
} from '../src/cli/flags.js'
import { renderHelp } from '../src/cli/help.js'
import { runCli, type CliSpec } from '../src/cli/run.js'
import { TEST_CA_PEM } from './helpers/ca.js'

const spec: CliSpec<unknown, Destination> = {
  command: 'workos-sync-test',
  technology: 'Testbase',
  rowNoun: 'row',
  connection: 'Testbase connection string',
  tlsCaVar: 'WORKOS_SYNC_TEST_TLS_CA',
  serviceName: 'workos-sync-test',
  init: { name: 'init [--repair] [--print-sql]', text: ['Create the schema.', 'Exits 2 on drift.'] },
  targetFlags: [{ name: '--schema <name>', text: ['Target schema (or WORKOS_SYNC_SCHEMA; default: workos)'] }],
  targetEnv: [{ name: 'WORKOS_SYNC_SCHEMA', text: ['Target schema (default: workos)'] }],
  printSchemaSql: (streams, flags) => `-- ${streams?.join(',') ?? 'all'} into ${String(flags['schema'] ?? 'workos')}\n`,
  destination: () => {
    throw new Error('no database in this test')
  },
}

describe('arguments', () => {
  it('reads a command, valued flags and switches, and defaults to help', () => {
    expect(parseArgs([])).toEqual({ command: 'help', flags: {} })
    expect(parseArgs(['backfill', '--resume', '--max-pages', '3', '--streams', 'users,roles'])).toEqual({
      command: 'backfill',
      flags: { resume: true, 'max-pages': '3', streams: 'users,roles' },
    })
    expect(streamsFromFlags({ streams: 'users,roles' })).toEqual(['users', 'roles'])
    expect(streamsFromFlags({})).toBeUndefined()
  })
})

describe('entrypoint', () => {
  let dir: string
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'workos-sync-bin-'))
  })
  afterEach(() => {
    rmSync(dir, { recursive: true, force: true })
  })

  it('recognises the script itself and the symlink npm installs as the command', () => {
    const script = join(dir, 'cli.js')
    writeFileSync(script, '')
    const url = pathToFileURL(script).href
    expect(invokedDirectly(script, url)).toBe(true)
    // `npm install` links `node_modules/.bin/workos-sync-<db>` at dist/cli.js,
    // so argv[1] is the link, not the file.
    const link = join(dir, 'workos-sync-test')
    symlinkSync(script, link)
    expect(invokedDirectly(link, url)).toBe(true)
  })

  it('is false for an import, a missing path, or another script', () => {
    const script = join(dir, 'cli.js')
    writeFileSync(script, '')
    const url = pathToFileURL(script).href
    expect(invokedDirectly(undefined, url)).toBe(false)
    expect(invokedDirectly(join(dir, 'missing'), url)).toBe(false)
    const other = join(dir, 'vitest.mjs')
    writeFileSync(other, '')
    expect(invokedDirectly(other, url)).toBe(false)
    // This test file was started by the test runner, not by the CLI.
    expect(invokedDirectly(process.argv[1], url)).toBe(false)
  })
})

describe('WORKOS_SYNC_HYDRATE', () => {
  it('leaves the engine default in place when unset', () => {
    expect(parseHydrate(undefined)).toBeUndefined()
    expect(parseHydrate('')).toBeUndefined()
  })

  it('accepts one mode for every stream', () => {
    expect(parseHydrate('off')).toBe('off')
    expect(parseHydrate('always')).toBe('always')
  })

  it('accepts per-stream overrides on top of the default', () => {
    expect(parseHydrate('users=always')).toEqual({ users: 'always' })
    expect(parseHydrate('users=off, organizations=off')).toEqual({ users: 'off', organizations: 'off' })
  })

  it('rejects anything else loudly', () => {
    expect(() => parseHydrate('sometimes')).toThrow(/invalid WORKOS_SYNC_HYDRATE/)
    expect(() => parseHydrate('users=maybe')).toThrow(/invalid WORKOS_SYNC_HYDRATE/)
  })
})

describe('backfill --max-pages', () => {
  it('means unbounded when absent', () => {
    expect(parseMaxPages(undefined)).toBeUndefined()
  })

  it('accepts a positive integer', () => {
    expect(parseMaxPages('25')).toBe(25)
    expect(parseMaxPages(' 1 ')).toBe(1)
  })

  it('refuses a bare flag, zero, a fraction, and anything that is not a number', () => {
    // `--max-pages` with no value parses as `true`; it must not become a budget of nothing.
    for (const bad of [true, '', '0', '-3', '1.5', 'ten']) {
      expect(() => parseMaxPages(bad)).toThrow(/invalid --max-pages/)
    }
  })
})

describe('reconcile --exclude-streams', () => {
  it('means nothing excluded when absent and splits a list', () => {
    expect(parseExcludeStreams(undefined)).toBeUndefined()
    expect(parseExcludeStreams('users, invitations')).toEqual(['users', 'invitations'])
  })

  it('refuses a bare flag: an exclusion of nothing is the mistake it exists to prevent', () => {
    expect(() => parseExcludeStreams(true)).toThrow(/invalid --exclude-streams/)
    expect(() => parseExcludeStreams(' , ')).toThrow(/invalid --exclude-streams/)
  })
})

describe('init --repair', () => {
  it('asks for repair on init and nothing else', () => {
    expect(repairRequested('init', { repair: true })).toBe(true)
    expect(repairRequested('init', {})).toBe(false)
    // `reconcile --repair` repairs rows, not views.
    expect(repairRequested('reconcile', { repair: true })).toBe(false)
  })
})

describe('WORKOS_SYNC_LEASE_TTL_MS', () => {
  it('leaves the engine default in place when unset or empty', () => {
    expect(parseLeaseTtl(undefined)).toBeUndefined()
    expect(parseLeaseTtl('')).toBeUndefined()
    expect(parseLeaseTtl('  ')).toBeUndefined()
  })

  it('accepts a number of milliseconds', () => {
    expect(parseLeaseTtl('250')).toBe(250)
  })

  it('refuses a value that would expire on arrival or fail in SQL', () => {
    // Number('') is 0 and Number('abc') is NaN. A zero TTL is a lease that
    // excludes nobody, so neither may reach the engine.
    for (const bad of ['0', '-1', '0.5', 'abc', 'Infinity']) {
      expect(() => parseLeaseTtl(bad)).toThrow(/invalid WORKOS_SYNC_LEASE_TTL_MS/)
    }
  })
})

describe('the CA certificate', () => {
  const pem = TEST_CA_PEM
  const parse = (value: string | undefined) => parseSslCa(value, 'WORKOS_SYNC_TEST_TLS_CA')

  it('leaves the driver defaults in place when unset or empty', () => {
    expect(parse(undefined)).toBeUndefined()
    expect(parse('')).toBeUndefined()
    expect(parse('  ')).toBeUndefined()
  })

  it('accepts PEM as is, and a bundle of several certificates', () => {
    expect(parse(pem)).toBe(pem.trim())
    expect(parse(pem + pem)).toBe((pem + pem).trim())
  })

  it('straightens a PEM whose newlines arrived as the two characters backslash-n', () => {
    expect(parse(pem.trim().replace(/\n/g, '\\n'))).toBe(pem.trim())
  })

  it('accepts the PEM encoded as base64, line-wrapped or not', () => {
    const b64 = Buffer.from(pem).toString('base64')
    expect(parse(b64)).toBe(pem)
    expect(parse(b64.replace(/(.{20})/g, '$1\n'))).toBe(pem)
  })

  it('refuses anything that is not a certificate, naming the variable', () => {
    // A path, prose, and base64 of something else: each would otherwise reach
    // the driver and fail there as a handshake error against the server.
    for (const bad of ['/etc/ssl/ca.crt', 'not a cert', Buffer.from('nope').toString('base64')]) {
      expect(() => parse(bad)).toThrow(/invalid WORKOS_SYNC_TEST_TLS_CA/)
    }
  })

  it('refuses a damaged certificate rather than letting the driver fail on it', () => {
    const lines = pem.trim().split('\n')
    const header = lines[0]!
    const footer = lines[lines.length - 1]!
    const unterminated = lines.slice(0, -1).join('\n')
    const truncated = [header, ...lines.slice(1, 4), footer].join('\n')
    const garbage = [header, Buffer.from('not a cert').toString('base64'), footer].join('\n')
    for (const bad of [header, unterminated, truncated, garbage, Buffer.from(truncated).toString('base64')]) {
      expect(() => parse(bad)).toThrow(/invalid WORKOS_SYNC_TEST_TLS_CA/)
    }
  })
})

describe('help', () => {
  it('is rendered from the spec, with the shared commands and the destination words', () => {
    const help = renderHelp(spec)
    expect(help).toContain('workos-sync-test — mirror WorkOS data into Testbase')
    expect(help).toContain(
      '  init [--repair] [--print-sql]\n                         Create the schema.\n                         Exits 2 on drift.',
    )
    expect(help).toContain('reconcile [--repair] [--resume] [--max-pages <n>] [--exclude-streams <a,b>]')
    expect(help).toContain('--database-url <url>   Testbase connection string (or DATABASE_URL)')
    expect(help).toContain('--schema <name>        Target schema')
    expect(help).toContain('WORKOS_SYNC_TEST_TLS_CA      CA certificate')
    expect(help).toContain('WORKOS_SYNC_SCHEMA           Target schema (default: workos)')
    expect(help).toContain('replacing each row')
    expect(help).toContain('WORKOS_SYNC_MAX_EVENTS_PER_TICK\n                               Events to apply per poll')
  })
})

describe('commands that need no database', () => {
  let out: string[]
  beforeEach(() => {
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      out.push(String(chunk))
      return true
    }) as typeof process.stdout.write)
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('prints the help for help, --help and no command at all', async () => {
    expect(await runCli(spec, [])).toBe(0)
    expect(await runCli(spec, ['help'])).toBe(0)
    expect(await runCli(spec, ['backfill', '--help'])).toBe(0)
    expect(out.join('')).toContain('mirror WorkOS data into Testbase')
  })

  it('lists the catalog for streams', async () => {
    expect(await runCli(spec, ['streams'])).toBe(0)
    const rows = JSON.parse(out.join('')) as { stream: string; default: string }[]
    expect(rows.some((r) => r.stream === 'users' && r.default === 'yes')).toBe(true)
  })

  it('prints the DDL for init --print-sql through the spec, with the flags', async () => {
    expect(await runCli(spec, ['init', '--print-sql', '--streams', 'users', '--schema', 'mirror'])).toBe(0)
    expect(out.join('')).toBe('-- users into mirror\n')
  })
})
