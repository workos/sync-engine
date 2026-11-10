# MongoDB Atlas

[Atlas](https://www.mongodb.com/atlas) is MongoDB's hosted service. The
engine's MongoDB destination reaches it over an ordinary connection string,
so this guide is about the choices Atlas puts in front of you: the cluster
tier, the database user's role, the IP access list, and which of its
connection strings to copy.

## Create the cluster

Create a cluster in a project set aside for the mirror, or in the project
your application already uses: the engine creates its own `workos`
database and touches nothing else.

| Tier                       | Suits                                                                                                            |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **Free** (M0)              | Evaluating the sync. 512 MB, shared CPU, 500 connections, no backups. Atlas pauses it after a long idle stretch. |
| **Flex**                   | A small mirror with a usage-based bill.                                                                          |
| **Dedicated** (M10 and up) | A production mirror: backups, metrics, private networking.                                                       |

Any tier runs MongoDB 8.0, well past the 5.0 the package needs. When the
template form offers **Automate security setup**, leave it on: it creates a
database user and adds your current address to the IP access list, the two
things the next sections are about. Turn **Preload sample dataset** off on
a free cluster, where the sample data would take most of the 512 MB.
Provisioning takes a few minutes, during which the driver reports a
server-selection timeout.

## Database user

Atlas authenticates with SCRAM users that live in the project, under
**Database Access**. The sync's user needs `readWrite` on the mirror's
database: that covers creating the collections, indexes and views,
writing to them, and dropping the staging collection `reconcile` builds.
Add `dbAdmin` on the same database if you will run `init --repair`, which
rewrites a view with `collMod`. The automated setup creates a user with
the **Atlas admin** role, which is more than the sync needs and fine for an
evaluation; for production, give a dedicated user the two roles on the one
database.

The password is shown once. If it is gone, **Database Access → Edit → Edit
Password → Autogenerate Secure Password → Update User** issues a new one
and takes a few seconds to deploy.

## Network access

Atlas refuses every address not on the project's **Network Access** list,
and a refused address looks like a connection timeout, not an error about
access. Add the address the sync connects from before the first `init`, and
again when it moves. A host with a changing address on a dedicated tier is
better served by a private endpoint or VPC peering than by opening the
list to everything.

## Choose a connection

**Connect → Drivers** shows the connection string:

```
mongodb+srv://USER:PASSWORD@CLUSTER.xxxxx.mongodb.net/
```

The `mongodb+srv://` form is a DNS seed list: the driver looks up the
replica set's hosts and options from DNS and connects to them on port 27017. Where DNS SRV lookups are not possible, the dialog's **Legacy URI
string** toggle shows the equivalent `mongodb://host1,host2,host3/...`
form with the hosts and `replicaSet` spelled out; the engine takes either.
Put the database user's password in (percent-encoded if it has characters
that mean something in a URL).

The mirror lives in the database the string names, or `workos` when it
names none; `WORKOS_SYNC_DATABASE` or `--database` overrides both.

## TLS

Atlas requires TLS, and `mongodb+srv://` turns it on. Atlas's certificates
chain to public roots Node already trusts, so `WORKOS_SYNC_MONGODB_TLS_CA`
stays unset and there is no `sslCa` to pass in embedded code.

## Verify the endpoint

From a checkout of this repository, after `pnpm install` and `pnpm build`:

```sh
TEST_MONGODB_URL='mongodb+srv://USER:PASSWORD@CLUSTER.xxxxx.mongodb.net/' \
  pnpm --filter @workos/sync-mongodb test
```

With that variable set, the suite runs against your cluster instead of the
embedded `mongod` it otherwise starts. Each test creates a
`workos_test_<hex>` database and drops it afterwards, which is why the
user running the suite needs Atlas admin (or `dropDatabase` on every
database); the sync itself does not. WorkOS responses are local fixtures,
so no key is involved.

Against a free M0 cluster in November 2026 the suite passed except for two
tests that assume a server a millisecond away: a lease test that stalls a
tick for 400 ms under a 150 ms lease, and a health test that stops
`listen()` 50 ms after starting it and expects a completed tick. Both are
timing assumptions in the tests rather than anything Atlas does.

## Run the sync from your own host

```sh
export DATABASE_URL='mongodb+srv://USER:PASSWORD@CLUSTER.xxxxx.mongodb.net/'
export WORKOS_API_KEY=sk_...

npx workos-sync-mongodb init
npx workos-sync-mongodb backfill
npx workos-sync-mongodb listen
```

The [container image](../../README.md#against-a-real-environment) and the
[library](../getting-started.md#embed-it) take the same URL. Two things
to plan for:

- **A free cluster must stay in use.** Atlas pauses an M0 that has had no
  connections for a long stretch and resumes it from the console. A
  running `listen` never leaves that gap; a `tick` on a schedule does not
  either, as long as it keeps running.
- **The IP access list must include the sync's host**, as above; a
  deployment that moves between addresses needs each of them listed, or a
  private endpoint.

## Query it

Everything in [Querying the mirror](../queries.md) applies, in
MongoDB's terms: the collections are `workos_users`, `workos_organizations`
and so on, each with a `workos_<name>_live` view that hides tombstones, and
the payload is under `_raw`. **Browse collections** in the console opens
Data Explorer on the `workos` database, and `mongosh` and Compass take the
same connection string:

```js
db.workos_users_live.find({ _env: "production" }, { "_raw.email": 1 });
```

The [`@workos/sync-mongodb` README](../../packages/sync-mongodb/README.md#querying-the-mirror)
has the `$lookup` that joins memberships to users and organizations, and
the indexes the engine creates for it.
