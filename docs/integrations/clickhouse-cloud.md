# ClickHouse Cloud

[ClickHouse Cloud](https://clickhouse.com/cloud) is ClickHouse's hosted
service. The engine's ClickHouse destination reaches it over the HTTP
interface with the official client, so this guide is about the choices the
console puts in front of you: the service tier, the user, the IP access
list, which endpoint to copy, and what idling and a second replica mean for
the sync.

## Create the service

Create a service in the console, in an organization set aside for the
mirror or the one your analytics already use: the engine creates its own
`workos` database and touches nothing else. Choose a cloud and a region
close to where the sync will run, and a tier:

| Tier           | Suits                                                                                                                   |
| -------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Basic**      | Evaluating the sync. One replica of a fixed size.                                                                       |
| **Scale**      | A production mirror. Two or more replicas, autoscaling, backups you configure. The tier this guide was written against. |
| **Enterprise** | Scale, plus scheduled upgrades, the slow release channel and compliance features.                                       |

A new organization starts on a trial with a credit allowance that runs out
after 30 days at the time of writing; the console's banner counts the days
down, and the service keeps running past them only once payment details are
in. Any tier runs a current ClickHouse (26.6 on the service used here), well
past the 23.11 the package needs, and every service has the Keeper and the
`keeper_map_path_prefix` that the engine's lease, cursor and write-lock
tables depend on. There is nothing to enable: `init` creates its
[KeeperMap](../../packages/sync-clickhouse/README.md#requirements) tables
like any other.

The `default` user's password is shown once, in the **Connect** dialog
when the service is created, and the dialog says so afterwards. If it is
gone, **Settings → Reset password** issues a new one and invalidates the
old one for every client that had it.

## Database user

The `default` user is the service's administrator and is what this guide
was verified with; it is more than the sync needs and fine for an
evaluation. For production, create a user in the SQL console with the
grants the
[package README](../../packages/sync-clickhouse/README.md#command-line)
lists: `CREATE DATABASE` the first time, or an existing database it may
create tables in, plus `CREATE TABLE`, `CREATE VIEW`, `DROP TABLE`, `ALTER`,
`INSERT` and `SELECT` on that database. `ALTER` covers the lightweight
`DELETE` that `redact` runs and the `ADD COLUMN` and `ADD INDEX` a later
release may need. The bookkeeping tables are KeeperMap tables, created with
the same `CREATE TABLE` grant. A dedicated user has not been tried against
Cloud; the suite below is how to confirm one.

## Network access

**Settings → Security → IP access list** is where the service decides who
may connect. A refused address looks like a connection timeout, not an
error about access, so add the address the sync connects from before the
first `init`, and again when it moves. The console warns that a service
open to anywhere can be woken by internet scanners, and a woken service
bills compute, which is a second reason to list addresses rather than open
it. A host with a changing address is better served by a private endpoint,
also under **Security**.

Only ports 8443 (HTTPS) and 9440 (the native protocol) are exposed. A
network that allows outbound traffic on 443 alone cannot reach the service
and shows the same timeout as a missing access-list entry. Tell the two
apart from a shell:

```sh
curl https://HOST.clickhouse.cloud:8443/ping   # prints "Ok." when the port is reachable
```

## Choose a connection

**Connect** on the service, with **HTTPS** selected, shows a `curl`
command whose last line is the endpoint:

```
https://abc123.us-east1.gcp.clickhouse.cloud:8443
```

The engine takes the same endpoint with the credentials in front:

```
https://default:PASSWORD@abc123.us-east1.gcp.clickhouse.cloud:8443
```

Percent-encode the password if it has characters that mean something in a
URL; the engine decodes it. A path in the URL is ignored: the mirror lives
in the `workos` database the engine creates, or whatever
`WORKOS_SYNC_SCHEMA` or `--schema` names. Only `http://` and `https://` are
accepted; the native protocol on 9440, which `clickhouse client` uses, is
not an option for the engine.

Every connection the engine opens carries the settings a multi-replica
service needs and nothing on the service has to change for them: inserts
are asynchronous but waited for, mutations return once every replica has
them, and reads are sequentially consistent, so the write path never reads
a replica the last insert has not reached.

## TLS

The endpoint is HTTPS, and Cloud's certificates chain to public roots that
Node already trusts, so `WORKOS_SYNC_CLICKHOUSE_TLS_CA` stays unset and
there is no `sslCa` to pass in embedded code. There is no plaintext port to
fall back to.

## Idling

A service idles after a stretch without queries, 15 minutes by default
(**Settings → Scaling → Idling**), and wakes on the next request, which
waits while it does. Two consequences for the sync:

- **`listen` keeps the service awake.** It polls every few seconds, so a
  service with a listener on it never idles and bills compute the whole
  time. That is the normal cost of a tail. `tick` on a schedule lets the
  service idle between runs at the price of a wake per run.
- **A cold first command waits for the wake.** The engine allows two
  minutes per request; a `status` or `init` against an idle service spends
  the first seconds of that waiting, which is not a failure.

## Verify the endpoint

From a checkout of this repository, after `pnpm install` and `pnpm build`:

```sh
TEST_CLICKHOUSE_URL='https://default:PASSWORD@abc123.us-east1.gcp.clickhouse.cloud:8443' \
  pnpm --filter @workos/sync-clickhouse test:database
```

With that variable set, the suite runs against your service instead of a
local server. Each test creates a `workos_test_<hex>` database and drops it
afterwards, which is why the user running the suite needs `CREATE DATABASE`
and `DROP DATABASE`; the KeeperMap tables keep their Keeper paths under the
database's name, so parallel tests never share a lease. WorkOS responses
are local fixtures, so no key is involved. Expect it to take around ten
minutes: every statement is an HTTPS round trip to the region.

Against a two-replica Scale service in November 2026 the suite passed
except for one test. That test applies the script `init --print-sql`
prints statement by statement, each as its own HTTP request, and on a
service with more than one replica a `CREATE VIEW` reached a replica before
the `CREATE TABLE` it selects from had been replicated to it. The engine's
own `init`, which the rest of the suite runs many times against the same
service, did not hit it. If you apply the printed script by hand and a
statement fails this way, run the script again: every statement in it is
idempotent, and the second pass completes what the first could not.

## Run the sync from your own host

```sh
export DATABASE_URL='https://default:PASSWORD@abc123.us-east1.gcp.clickhouse.cloud:8443'
export WORKOS_API_KEY=sk_...

npx workos-sync-clickhouse init
npx workos-sync-clickhouse backfill
npx workos-sync-clickhouse listen
```

The [container image](../../README.md#against-a-real-environment) and the
[library](../getting-started.md#embed-it) take the same URL. Two things to
plan for:

- **Start the tail early.** The sign-in history in `auth_attempts` is the
  reason to mirror into ClickHouse, and it begins the day `listen` first
  runs: WorkOS keeps events for 90 days and has no endpoint that lists past
  sign-ins, so `backfill` cannot recover them.
- **The service will not idle while `listen` runs**, as above. Size the
  service for a process that is always connected, and raise
  `WORKOS_SYNC_MAX_EVENTS_PER_TICK` so each poll is a few larger inserts
  rather than many small ones.

## Query it

Everything in [Querying the mirror](../queries.md) applies. The service's
**SQL console** lists the `workos` database next to `default` once `init`
has run; the `_live` views read with `FINAL` and hide tombstones, and the
base tables hold every version a row has had. From your own machine,
`clickhouse client` takes the host with `--secure` and the password, and
`curl` takes the endpoint from **Connect** with a query as its body:

```sql
SELECT _env, email FROM workos.users_live;
```

The [`@workos/sync-clickhouse` README](../../packages/sync-clickhouse/README.md)
opens with the query this destination exists for: failed sign-ins per
organization per hour, joined across the mirror tables in one database.
