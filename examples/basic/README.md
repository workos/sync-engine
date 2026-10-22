# End-to-end example

A complete walkthrough of the sync engine, runnable with no credentials, no
Docker, and no network access.

```bash
pnpm install
pnpm build      # the example imports the built packages
pnpm start
```

It runs against a stand-in WorkOS API served over real HTTP and an embedded
Postgres (PGlite, which is real Postgres compiled to WebAssembly). Everything
else is the production code path: the same schema, the same write path, the
same cursor handling, the same reconciler.

## What it demonstrates

1. **Schema creation** prints the `workos.users` column list so you can see
   that `_raw` is the only written column and everything else is generated
   from it.

2. **Backfill** loads organizations, users, memberships, domains, directories
   and directory users that existed before the engine was installed. This is
   the data a webhook-only integration can never recover.

3. **The tail** applies live changes: an update, a new hire, three sign-in
   attempts, a deletion, and one event type the engine has never seen. The
   unknown event is archived rather than throwing. The stand-in API also
   emitted events for everything the backfill already loaded, so the first
   poll replays those too; the staleness gate skips them, which is the
   `skipped` count below doing its job.

4. **Queries** answer members and roles per organization; failed sign-ins by
   user (a history with no REST endpoint anywhere in the API); organizations
   with a verified domain but no SCIM directory; and identity data joined to an
   application-owned `app_usage` table.

5. **Drift and repair** deletes one user and creates another _without_
   emitting events, so the event stream cannot see either. Reconciliation finds
   both, `--repair` fixes them, and a second pass confirms the mirror is clean.

6. **Operational view** shows cursor position and age, plus per-stream row
   counts, tombstone counts, and lag. The lag reads about a minute because the
   stand-in API stamps its objects a minute in the past, not because the sync
   is slow.

## Expected output

```
1. Create the schema
   tables: auth_attempts, directories, directory_users, events,
           organization_domains, organization_memberships, organizations, users

2. Backfill what already exists
   organizations 2 · users 3 · directories 1 · organization_memberships 3
   organization_domains 2 · directory_users 1

3. Apply live changes from the Events API
   processed 17 events
   writes: { created: 5, updated: 1, skipped: 9, deleted: 2 }

4. Ask questions that need a join
   ...

5. Detect and repair drift
   in the mirror but not upstream (missed deletes): 1
   upstream but not in the mirror (missed creates): 1
   after repair — localOnly: 0, remoteOnly: 0

6. Operational view
   cursor: event_01HF7UBMA80000000000000029
   cursor age: 0.0007 days (alarm at 75, data gone at 90)
```

## Pointing it at a real environment

Replace the `clientFor` override and the `apiUrl` with your own key, and drop
the fake entirely:

```ts
const engine = new SyncEngine({
  destination: new PostgresDestination({
    databaseUrl: process.env.DATABASE_URL,
  }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});
```

Start with `backfill` on a staging environment first. It is the only step that
issues a large number of API calls.
