/**
 * A database per test on the server the global setup provides, so the tests
 * exercise real update pipelines, `$lookup`, views and unique indexes — not a
 * mock's approximation — and never see each other's state.
 *
 * One client serves every test in a file: a client is a pool with its own
 * monitors, and opening forty of them in one worker is what keeps that
 * worker from exiting promptly once the file is done. `test/setup.ts` closes
 * it after the file's last test.
 */
import { randomBytes } from 'node:crypto'
import { MongoClient, type Db } from 'mongodb'

export interface TestDb {
  client: MongoClient
  db: Db
  /** The database name, for `new MongoDestination({ client, database })`. */
  name: string
  /** Drop the database. The client is shared and stays open. */
  close(): Promise<void>
}

let shared: Promise<MongoClient> | undefined

async function sharedClient(): Promise<MongoClient> {
  const url = process.env['TEST_MONGODB_URL']
  if (!url) throw new Error('TEST_MONGODB_URL is not set; the global setup should have started a server')
  shared ??= new MongoClient(url).connect()
  return shared
}

export async function createTestDb(): Promise<TestDb> {
  const client = await sharedClient()
  const name = `workos_test_${randomBytes(6).toString('hex')}`
  const db = client.db(name)
  return {
    client,
    db,
    name,
    async close() {
      await db.dropDatabase()
    },
  }
}

/** Close the file's client. Registered by `test/setup.ts` as an `afterAll`. */
export async function closeSharedClient(): Promise<void> {
  const client = await shared?.catch(() => undefined)
  shared = undefined
  // Forced: nothing is in flight once the file's last test has returned.
  await client?.close(true)
}
