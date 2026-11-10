# PlanetScale

[PlanetScale](https://planetscale.com) hosts two engines, and the sync has a
package for each. Both are reached over the engine's ordinary protocol: no
PlanetScale SDK, extension or superuser is involved, only the choices below
about roles, connection strings and TLS.

| PlanetScale engine                       | Package                                                 | Notes                                                                              |
| ---------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **Postgres** (18 at the time of writing) | [`@workos/sync-postgres`](../../packages/sync-postgres) | Ordinary Postgres. Everything in [Hosted Postgres](../hosted-postgres.md) applies. |
| **Vitess** (MySQL 8.4 behind Vitess)     | [`@workos/sync-mysql`](../../packages/sync-mysql)       | MySQL-compatible, with two differences that matter to the sync, below.             |

Neki, PlanetScale's sharded Postgres, was in preview at the time of writing
and has not been tried.

## Create the database

Creating any database needs a credit card on the organization, and every
database bills hourly from creation, so create it when you are ready to
use it and delete it from its settings when an evaluation is done.

In the console, **Create a new database**, then choose the engine, a
region close to where the sync will run, and a size:

- **Postgres** offers **Single node** or **Primary + multi-replica**. A
  single-node PS-5 is enough to evaluate the sync, at $5 a month when this
  was written; the three-node PS-5 was $15 and carries the availability
  SLA.
- **Vitess** is always three nodes: a primary and two replicas behind
  three VTGate proxies. The smallest, PS-10, was $39 a month.

The database gets a `main` branch that takes a minute or two to initialize;
until it has, the **Connect** page's buttons stay disabled. Credentials are
scoped to a branch, so every branch the sync should reach gets its own
role or password, created from that branch's **Connect** page.

## Postgres

### Create a role

The role the **Connect** page offers first is a member of
`pg_read_all_data` and `pg_write_all_data` and nothing else. It can read
and write every table, but it has no `CREATE` on the database, so `init`
fails at its first statement:

```
permission denied for database postgres
```

Instead, choose **Create new role**, keep **User-defined role**, and tick
**`postgres`** ("create, modify, and drop databases, users, roles, tables,
schemas, and all other objects"). That is the membership `init` needs to
create the `workos` schema and own its objects. Leave **Query safety**
(`pg_strict`) off for this role; the compatibility suite below was run
without it. The next screen shows the username, password and connection
URI once.

### Choose a connection

The URI PlanetScale shows is the direct endpoint:

```
postgresql://USER:PASSWORD@aws-<region>-N.pg.psdb.cloud:5432/postgres?sslmode=verify-full&sslrootcert=system
```

Drop the final `sslrootcert=system` parameter. It is a libpq option that
tells `psql` to use the operating system's trust store; Node's `pg` reads
it as a file path and fails with `ENOENT: open 'system'`. Node uses the
system store without being told.

The dashboard also shows a **PgBouncer** endpoint next to the direct one,
with a far higher connection limit (400 against 25 on a PS-5 when this
was written). The direct endpoint has passed the compatibility suite;
PgBouncer in transaction mode is expected to work for the reasons
[Hosted Postgres](../hosted-postgres.md#choose-a-connection) gives,
and the suite below is how to confirm it for the endpoint you choose.

### TLS

Keep `sslmode=verify-full`. PlanetScale's certificates chain to public
roots that Node already trusts, so `WORKOS_SYNC_PG_SSL_CA` stays unset and
there is no `sslCa` to pass in embedded code.

One thing not to be alarmed by: PlanetScale terminates TLS at its proxy,
so `pg_stat_ssl` on the backend reports `ssl = false` for your session even
though the client socket is encrypted. A check that reads that view will
say TLS is off; it is not.

### Verify the endpoint

From a checkout of this repository, after `pnpm install` and `pnpm build`:

```sh
TEST_DATABASE_URL='postgresql://USER:PASSWORD@aws-<region>-N.pg.psdb.cloud:5432/postgres?sslmode=verify-full' \
  pnpm --filter @workos/sync-postgres test:database
```

The suite creates a uniquely named schema per test and drops it, exercising
every stream definition, generated columns, live views, transactions,
reconciliation, writer leases, checkpoints and tombstones against the real
endpoint, with local fixtures standing in for WorkOS. It passed against a
single-node PS-5 in November 2026.

### Run the sync

```sh
export DATABASE_URL='postgresql://USER:PASSWORD@aws-<region>-N.pg.psdb.cloud:5432/postgres?sslmode=verify-full'
export WORKOS_API_KEY=sk_...

npx workos-sync-postgres init
npx workos-sync-postgres backfill
npx workos-sync-postgres listen
```

The [container image](../../README.md#against-a-real-environment) and the
[library](../getting-started.md#embed-it) take the same URL. Budget for
the connection limit of the size you chose: `listen` holds a small pool
open for as long as it runs, and a PS-5's 25 direct connections are shared
with everything else that connects. Move to the PgBouncer endpoint, or a
larger size, before the limit rather than after.

## Vitess (MySQL)

Vitess speaks the MySQL protocol and runs MySQL 8.4 underneath, which meets
the package's 8.0.21 requirement. Two things about it shape the setup:

- **The branch is the database.** There is no `CREATE DATABASE`; a
  connection lands in the branch's one database, named after the
  PlanetScale database. The engine's default of creating a `workos`
  database of its own cannot work here, so name the branch database as the
  mirror's schema, with `--schema` or `WORKOS_SYNC_SCHEMA`. `init` sees it
  exists and creates the tables inside it.
- **Safe migrations gate DDL.** A branch with safe migrations on refuses
  schema changes outside a deploy request. The `main` branch of a new
  database starts with safe migrations off, so `init` applies its DDL
  directly there; turn it on for a production branch and the
  [`init --print-sql` path](../../packages/sync-mysql/README.md#command-line)
  in the package README is how the schema goes in.

A third one is historical: Vitess answers `GET_LOCK` itself and names the
result column after the expression rather than its alias. `@workos/sync-mysql`
0.0.1 read it by alias and failed every command on Vitess with
`could not take the migration lock`; 0.1.0 and later read it by position.

### Create a password

On the branch's **Connect** page, keep connection type **Primary**, choose
role **Admin**, and **Create password**. `init` needs DDL, and the Admin
role is the one that carries it. The username and password are shown
once; the **Node.js** tab shows them as a connection string.

### Choose a connection

PlanetScale offers two hosts for the same branch:

| Host                          | Notes                                                                 |
| ----------------------------- | --------------------------------------------------------------------- |
| `aws.connect.psdb.cloud`      | Latency-optimized: PlanetScale routes to the branch from its edge.    |
| `<region>.connect.psdb.cloud` | Direct to the region, for networks where the edge route adds latency. |

Either works. Use port 3306 and the branch database as the path, and drop
the `?ssl={"rejectUnauthorized":true}` parameter from the string PlanetScale
shows; TLS is configured through the engine's own variable, below:

```
mysql://USER:PASSWORD@aws.connect.psdb.cloud:3306/DATABASE
```

### TLS

PlanetScale refuses plaintext connections, and this package only turns TLS
on when it is given a CA to verify against. PlanetScale's certificates
chain to public roots, so the CA is the operating system's bundle:

```sh
export WORKOS_SYNC_MYSQL_SSL_CA="$(cat /etc/ssl/cert.pem)"                   # macOS
export WORKOS_SYNC_MYSQL_SSL_CA="$(cat /etc/ssl/certs/ca-certificates.crt)"  # Debian, Ubuntu
```

In embedded code, pass the same text as `sslCa` to `MysqlDestination`.

### Verify the endpoint

The package's database-backed suite cannot run against Vitess: it creates
a database per test, which Vitess has no statement for. Verify with the
command instead, against a development branch or a `main` with safe
migrations off, and a throwaway WorkOS environment:

```sh
export DATABASE_URL='mysql://USER:PASSWORD@aws.connect.psdb.cloud:3306/DATABASE'
export WORKOS_SYNC_MYSQL_SSL_CA="$(cat /etc/ssl/cert.pem)"
export WORKOS_SYNC_SCHEMA=DATABASE
export WORKOS_API_KEY=sk_test_...

npx workos-sync-mysql init          # creates the tables and views inside the branch database
npx workos-sync-mysql init          # second run: nothing to apply
npx workos-sync-mysql backfill
npx workos-sync-mysql tick
npx workos-sync-mysql reconcile     # exit 0: nothing drifted
npx workos-sync-mysql status
```

That lifecycle passed against a PS-10 in November 2026, generated columns,
`_live` views, temporary tables and the `GET_LOCK` migration lock included.

### Run the sync

```sh
export DATABASE_URL='mysql://USER:PASSWORD@aws.connect.psdb.cloud:3306/DATABASE'
export WORKOS_SYNC_MYSQL_SSL_CA="$(cat /etc/ssl/cert.pem)"
export WORKOS_SYNC_SCHEMA=DATABASE
export WORKOS_API_KEY=sk_...

npx workos-sync-mysql init
npx workos-sync-mysql backfill
npx workos-sync-mysql listen
```

With safe migrations on, run `init --print-sql --schema DATABASE`, drop its
first statement (the database exists), open a deploy request with the
rest, and run `init` once it has deployed; it then records the fingerprint
with no DDL of its own. A later release that adds a column needs the same
round trip, and `init` reports the missing column and the DDL for it.

## Branches

A PlanetScale branch is a separate database with its own credentials:

- **Create a role or password per branch.** A Postgres role and a Vitess
  password each connect to one branch only.
- **A branch starts without the mirror.** Run `init` and `backfill` on
  each branch that should carry one; there is no copy of the parent's
  cursor to resume from. Two branches syncing the same environment are two
  syncs on one key, and share its
  [rate limit](../operations.md#rate-limits).
- **Vitess development branches are where schema changes are made.** Run
  `init` on a development branch, let PlanetScale diff it against
  production, and deploy it; that is the deploy-request path above.

## Query it

Everything in [Querying the mirror](../queries.md) applies. The
console's **Console** entry is a web `psql` or `mysql` session on the
branch, and **Insights** shows query performance. From your own machine,
`psql` takes the URI PlanetScale shows (with `sslrootcert=system`, which
is exactly what libpq wants), and `mysql` takes
`--ssl-mode=VERIFY_IDENTITY` with the host, user and password:

```sql
-- Postgres
select email from workos.users_live;
-- Vitess: the branch database is already selected
select email from users_live;
```
