#!/usr/bin/env node
/**
 * workos-sync-mongodb — the command line.
 *
 * The commands, their flags, their exit codes and the way the environment
 * is read are the engine's (`runCli`); this file says what MongoDB does
 * differently: how its destination is built, what `init` reports,
 * and the words the help text needs.
 */
import { runCli, exitWith, invokedDirectly, type CliFlags, type CliSpec, type MainDeps } from '@workos/sync-engine'
import type { MongoClient } from 'mongodb'
import { MongoDestination } from './destination.js'
import { DEFAULT_PREFIX } from './schema/names.js'

function databaseFromFlags(flags: CliFlags): string | undefined {
  const v = (flags['database'] as string | undefined) ?? process.env['WORKOS_SYNC_DATABASE']
  return v && v.trim() ? v.trim() : undefined
}

function prefixFromFlags(flags: CliFlags): string {
  return (flags['prefix'] as string) ?? process.env['WORKOS_SYNC_PREFIX'] ?? DEFAULT_PREFIX
}

export const spec: CliSpec<MongoClient, MongoDestination> = {
  command: 'workos-sync-mongodb',
  technology: 'MongoDB',
  rowNoun: 'document',
  connection: 'MongoDB connection string (mongodb:// or mongodb+srv://)',
  tlsCaVar: 'WORKOS_SYNC_MONGODB_TLS_CA',
  serviceName: 'workos-sync-mongodb',
  init: {
    name: 'init [--repair]',
    text: [
      'Create the collections, indexes, _live views and bookkeeping.',
      'Fails if a generated _live view has a pipeline that is not the',
      'generated one; --repair redefines it',
    ],
  },
  targetFlags: [
    {
      name: '--database <name>',
      text: [
        'Database the mirror lives in (or WORKOS_SYNC_DATABASE;',
        'default: the one the connection string names, else workos)',
      ],
    },
    { name: '--prefix <prefix>', text: ['Collection prefix (or WORKOS_SYNC_PREFIX; default: workos_)'] },
  ],
  targetEnv: [
    { name: 'WORKOS_SYNC_DATABASE', text: ['Database the mirror lives in (default: from the connection string)'] },
    { name: 'WORKOS_SYNC_PREFIX', text: ['Collection prefix (default: workos_)'] },
  ],
  // Built apart from the rest of the config because `init` reads the report
  // back off it after the engine has run; the engine's own `init()` returns
  // nothing.
  destination: (flags, { databaseUrl, sslCa, db, syncId, repair }) =>
    new MongoDestination({
      url: databaseUrl,
      sslCa,
      client: db,
      database: databaseFromFlags(flags),
      prefix: prefixFromFlags(flags),
      syncId,
      repairViews: repair,
    }),
  initReport: (destination) => {
    const orphans = destination.lastMigration?.orphans ?? []
    const views = destination.lastMigration?.views ?? []
    // Orphans are advisory: an index the catalog no longer calls for is
    // still serving whatever query it was made for.
    return { output: orphans.length > 0 || views.length > 0 ? { views, orphans } : undefined, failed: false }
  },
}

export type { MainDeps }

export function main(argv = process.argv.slice(2), deps: MainDeps<MongoClient> = {}): Promise<number> {
  return runCli(spec, argv, deps)
}

if (invokedDirectly(process.argv[1], import.meta.url)) exitWith(main())
