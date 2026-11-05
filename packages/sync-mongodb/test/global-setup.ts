/**
 * One MongoDB for the whole run.
 *
 * `TEST_MONGODB_URL` points the suite at a server of your own (a container,
 * Atlas); without it, an embedded `mongod` from mongodb-memory-server is
 * started here, once, and every test file connects to it. Each test still
 * owns its state: `createTestDb` hands out a database of its own and drops
 * it afterwards. The binary is downloaded on first use and cached, since its
 * postinstall is skipped at `pnpm install`.
 */
import { MongoMemoryServer } from 'mongodb-memory-server'

export default async function setup(): Promise<(() => Promise<void>) | undefined> {
  if (process.env['TEST_MONGODB_URL']) return undefined
  const server = await MongoMemoryServer.create()
  process.env['TEST_MONGODB_URL'] = server.getUri()
  return async () => {
    // Nothing in the data directory is worth a graceful shutdown, and after a
    // full run has created and dropped a few thousand collections mongod
    // takes longer to stop than the ten seconds mongodb-memory-server allows
    // before it kills it anyway, with a warning. Kill it outright; `stop`
    // then only has the directory to clean up.
    server.instanceInfo?.instance.mongodProcess?.kill('SIGKILL')
    await server.stop()
  }
}
