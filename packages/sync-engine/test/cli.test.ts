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
import { PassThrough, Writable } from 'node:stream'
import { pathToFileURL } from 'node:url'
import type { Destination } from '../src/destination.js'
import type { SyncEngine } from '../src/engine.js'
import type { DiffResult } from '../src/sync/reconcile.js'
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
import { exitWith, runCli, type CliSpec } from '../src/cli/run.js'
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

  it('leaves the driver defaults in place when unset', () => {
    expect(parse(undefined)).toBeUndefined()
  })

  it('refuses a value that is set but empty, which is what a mistyped path leaves behind', () => {
    // `VAR="$(cat /wrong/path)"`. Read as unset, the verification that was
    // asked for is dropped without a word, and with a driver that only uses
    // TLS when given a CA, so is the encryption.
    for (const empty of ['', '  ', '\n']) {
      expect(() => parse(empty)).toThrow(/invalid WORKOS_SYNC_TEST_TLS_CA: set but empty/)
    }
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

  it('promises a read-only status only for a destination that says it has one', () => {
    // Without `ready()` the engine runs `init` before it reads, so the
    // shared text must not tell every destination's user that it will not.
    const plain = renderHelp(spec)
    expect(plain).toContain('sets the mirror up first, as init does, if it is not there')
    expect(plain).not.toContain('reads only')
    const readOnly = renderHelp({ ...spec, statusReadsOnly: true })
    expect(readOnly).toContain('reads only, and says so when the mirror is not there')
    expect(readOnly).not.toContain('sets the mirror up first')
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

describe('commands run against an engine', () => {
  let out: string[]
  let err: string[]
  beforeEach(() => {
    out = []
    err = []
    vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      out.push(String(chunk))
      return true
    }) as typeof process.stdout.write)
    vi.spyOn(process.stderr, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      err.push(String(chunk))
      return true
    }) as typeof process.stderr.write)
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })

  const diff = (over: Partial<DiffResult> = {}): DiffResult => ({
    stream: 'users',
    matched: 0,
    localOnly: [],
    remoteOnly: [],
    pages: 1,
    complete: true,
    ...over,
  })
  const reconciling = (...diffs: DiffResult[]) =>
    ({
      reconcile: async () => ({ freshness: {}, diffs: { prod: diffs } }),
      close: async () => {},
    }) as unknown as SyncEngine

  it('exits 0 for a clean reconcile and 2 for drift, unless the drift was repaired', async () => {
    expect(await runCli(spec, ['reconcile'], { engine: reconciling(diff()) })).toBe(0)
    const drifted = diff({ localOnly: ['user_01'] })
    expect(await runCli(spec, ['reconcile'], { engine: reconciling(drifted) })).toBe(2)
    expect(await runCli(spec, ['reconcile', '--repair'], { engine: reconciling(drifted) })).toBe(0)
  })

  it('exits 1 when a stream failed: nothing about it was checked', async () => {
    // Before this, a run in which every stream was refused by the database
    // exited 0, and a scheduled check read that as a clean mirror.
    const failed = diff({ complete: false, failed: true, skippedReason: 'failed: Error: DDL command denied' })
    expect(await runCli(spec, ['reconcile'], { engine: reconciling(failed) })).toBe(1)
    // Whatever the other streams found, and whether or not it was repaired.
    const drifted = diff({ stream: 'organizations', localOnly: ['org_01'] })
    expect(await runCli(spec, ['reconcile'], { engine: reconciling(drifted, failed) })).toBe(1)
    expect(await runCli(spec, ['reconcile', '--repair'], { engine: reconciling(failed) })).toBe(1)
  })

  it('still exits 0 for a stream a page budget left for a later slice', async () => {
    const waiting = diff({ complete: false, pages: 0, skippedReason: 'page budget spent before this stream' })
    const oversized = diff({ complete: false, oversized: true })
    expect(await runCli(spec, ['reconcile', '--max-pages', '1'], { engine: reconciling(waiting, oversized) })).toBe(0)
  })

  it('says on stderr when status found no mirror, and keeps stdout to the JSON', async () => {
    const reporting = (initialized: boolean) =>
      ({ status: async () => ({ prod: { initialized } }), close: async () => {} }) as unknown as SyncEngine

    expect(await runCli(spec, ['status'], { engine: reporting(false) })).toBe(0)
    expect(JSON.parse(out.join(''))).toEqual({ prod: { initialized: false } })
    expect(err.join('')).toContain('run init')

    out.length = 0
    err.length = 0
    expect(await runCli(spec, ['status'], { engine: reporting(true) })).toBe(0)
    expect(err.join('')).toBe('')
  })
})

describe('the exit', () => {
  it('waits for output already written before it exits', async () => {
    // A pipe is written asynchronously on macOS: `process.exit` straight
    // after a large write cut `init --print-sql | cat` off at 64 KB.
    const order: string[] = []
    const slow = new Writable({
      write(chunk: Buffer, _encoding, callback) {
        setTimeout(() => {
          if (chunk.length > 0) order.push('written')
          callback()
        }, 20)
      },
    })
    slow.write('x'.repeat(100_000))
    await exitWith(Promise.resolve(0), {
      exit: (code) => order.push(`exit ${code}`),
      stdout: slow,
      stderr: new PassThrough(),
    })
    expect(order).toEqual(['written', 'exit 0'])
  })

  it('prints a failure and exits 1, without waiting on a reader that went away', async () => {
    const order: string[] = []
    const printed: string[] = []
    // `| head`: the reader closed, and every write now fails with EPIPE.
    const gone = new Writable({
      write(_chunk, _encoding, callback) {
        callback(Object.assign(new Error('write EPIPE'), { code: 'EPIPE' }))
      },
    })
    const stderr = new Writable({
      write(chunk: Buffer, _encoding, callback) {
        printed.push(String(chunk))
        callback()
      },
    })
    await exitWith(Promise.reject(new Error('boom')), {
      exit: (code) => order.push(`exit ${code}`),
      stdout: gone,
      stderr,
    })
    expect(printed.join('')).toBe('boom\n')
    expect(order).toEqual(['exit 1'])
  })
})
