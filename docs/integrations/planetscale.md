# PlanetScale

[PlanetScale](https://planetscale.com) hosts two engines, and the sync has a
package for each. Both are reached over the engine's ordinary protocol: no
PlanetScale SDK, extension or superuser is involved, only the choices below
about roles, connection strings and TLS.

| PlanetScale engine                       | Package                                                 | Notes                                                                              |
| ---------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **Postgres** (18 at the time of writing) | [`@workos/sync-postgres`](../../packages/sync-postgres) | Ordinary Postgres. Everything in [Hosted Postgres](../hosted-postgres.md) applies. |
| **Vitess** (MySQL 8.4 behind Vitess)     | [`@workos/sync-mysql`](../../packages/sync-mysql)       | MySQL-compatible, with differences that matter to the sync, below.                 |

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
create the `workos` schema and own its objects. The next screen shows the
username, password and connection URI once.

The form shows **Query safety** options only when the `pg_strict`
extension is enabled on the branch, and they default to off. The
compatibility suite below ran without them. In PlanetScale's own testing
with both turned on for the sync's role, `status`, `tick`, `backfill` and
`listen` ran and nothing was blocked; `init` was not tried that way.

### Choose a connection

The URI PlanetScale shows is the direct endpoint. `HOST` below stands for
the host it issued for the database, which ends in `.pg.psdb.cloud`:

```
postgresql://USER:PASSWORD@HOST:5432/postgres?sslmode=verify-full&sslrootcert=system
```

Drop the final `sslrootcert=system` parameter. It is a libpq option that
tells `psql` to use the operating system's trust store; Node's `pg` reads
it as a file path and every command fails with
`ENOENT: no such file or directory, open 'system'`. Node uses the system
store without being told.

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
TEST_DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/postgres?sslmode=verify-full' \
  pnpm --filter @workos/sync-postgres test:database
```

The suite creates a uniquely named schema per test and drops it, exercising
every stream definition, generated columns, live views, transactions,
reconciliation, writer leases, checkpoints and tombstones against the real
endpoint, with local fixtures standing in for WorkOS. It passed against a
single-node PS-5 in November 2026.

### Run the sync

Install the package first. `npx workos-sync-postgres` runs the command
the package installs; with nothing installed, npm looks for a package by
the command's name, and there is none.

```sh
npm install @workos/sync-postgres

export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/postgres?sslmode=verify-full'
export WORKOS_API_KEY=sk_...
export WORKOS_ENVIRONMENT_NAME=production   # the environment the key belongs to

npx workos-sync-postgres init
npx workos-sync-postgres backfill
npx workos-sync-postgres listen
```

`WORKOS_ENVIRONMENT_NAME` is the label written to every row as `_env`. It
defaults to `production` whatever key is in use, and it is part of every
primary key, so set it before the first write and keep it the same for
every command;
[Getting started](../getting-started.md#create-the-schema) has the detail.

The [container image](../../README.md#against-a-real-environment) and the
[library](../getting-started.md#embed-it) take the same URL. Budget for
the connection limit of the size you chose: `listen` holds a small pool
open for as long as it runs, and a PS-5's 25 direct connections are shared
with everything else that connects. Move to the PgBouncer endpoint, or a
larger size, before the limit rather than after.

## Vitess (MySQL)

Vitess speaks the MySQL protocol and runs MySQL 8.4 underneath, which meets
the package's 8.0.21 requirement. Four things about it shape the setup:

- **The branch is the database.** There is no `CREATE DATABASE`; a
  connection lands in the branch's one database, named after the
  PlanetScale database. The engine's default of creating a `workos`
  database of its own cannot work here, so name the branch database as the
  mirror's schema, with `--schema` or `WORKOS_SYNC_SCHEMA`. `init` sees it
  exists and creates the tables inside it. Without it, or with a name
  that is not the branch's database, every command fails on the
  `create database` Vitess refuses.
- **The mirror shares that database with your tables.** Its tables have
  plain names, `users` among them, and a table of yours with the same
  name is taken for the mirror's. [Table names](#table-names) below lists
  them and says how to prefix them.
- **The keyspace has to be unsharded.** `reconcile` and the schema check
  use temporary tables, which Vitess does not support in a sharded
  keyspace: in PlanetScale's testing every command failed there with
  `Temporary table not supported in sharded database`, after `init` had
  created the tables. In a database that has both kinds, name an unsharded
  keyspace as `WORKOS_SYNC_SCHEMA`; the sync ran that way.
- **Safe migrations gate DDL.** A branch with safe migrations on refuses
  schema changes outside a deploy request. The `main` branch of a new
  database starts with safe migrations off, so `init` applies its DDL
  directly there; turn it on for a production branch and the schema goes
  in through deploy requests, as [Safe migrations](#safe-migrations)
  below describes.

One more is historical: Vitess answers `GET_LOCK` itself and names the
result column after the expression rather than its alias. `@workos/sync-mysql`
0.0.1 read it by alias and failed every command on Vitess with
`could not take the migration lock`; 0.1.0 and later read it by position.

### Table names

The mirror's tables go into the branch database beside your own, under
these names:

- One table per stream: `users`, `organizations`,
  `organization_memberships`, `organization_domains`, `organization_roles`,
  `invitations`, `sessions`, `directories`, `directory_users`,
  `directory_groups`, `directory_group_memberships`, `connections`,
  `groups`, `group_memberships`, `roles`, `permissions`, `feature_flags`,
  `events` and `auth_attempts`, plus `fga_resources` and
  `webhook_endpoints` when those streams are selected with `--streams`.
- A `_live` view for each of those except `events` and `auth_attempts`.
- Five bookkeeping tables: `_sync_state`, `_migrations`, `_sync_lease`,
  `_rejected` and `_columns`.

A table of yours under one of these names is taken for the mirror's.
`init` tries to add its generated columns to it and fails on the first,
`Unknown column '_raw' in 'generated column function'`, leaving your
table as it was and the bookkeeping tables behind.

To keep the two apart, give everything the mirror creates a prefix, with
`--table-prefix` or `WORKOS_SYNC_TABLE_PREFIX` (in embedded code,
`tablePrefix`). It is in releases after 0.1.0, and takes lowercase letters,
digits and underscores, up to 24 characters:

```sh
export WORKOS_SYNC_TABLE_PREFIX=workos_
```

The stream tables become `workos_users` and so on, the views
`workos_users_live`, and the bookkeeping tables `workos__sync_state` and
so on. Set it before the first `init` and keep it the same for every
command, like the environment name: a different prefix is a different,
empty mirror. On 0.1.0, which has no prefix, check for these names before
running `init`.

### Create a password

On the branch's **Connect** page, keep connection type **Primary**, choose
role **Admin**, and **Create password**. `init` needs DDL, and the Admin
role is the one that carries it. The username and password are shown
once; the **Node.js** tab shows them as a connection string.

### Choose a connection

Use the host PlanetScale shows with the password. Its documentation also
lists `aws.connect.psdb.cloud` and a `<region>.connect.psdb.cloud` for
every database, but a password is not certain to be accepted on a host
other than the one shown with it: in PlanetScale's testing of this guide,
on one database, both answered `Access denied`.

Use port 3306 and the branch database as the path, and keep the
`?ssl={"rejectUnauthorized":true}` parameter the string PlanetScale shows
ends with, which is what turns TLS on:

```
mysql://USER:PASSWORD@HOST:3306/DATABASE?ssl={"rejectUnauthorized":true}
```

### TLS

PlanetScale refuses plaintext connections, so the connection has to ask
for TLS. Either of two settings does that:

- **The `ssl` parameter in the connection string.** `mysql2`, the driver
  underneath, reads `ssl={"rejectUnauthorized":true}` from the URL, turns
  TLS on, and verifies the server's certificate against the roots Node
  already trusts, which PlanetScale's certificates chain to. Quote the
  whole string in a shell: it holds braces and double quotes. The examples
  below use this.
- **`WORKOS_SYNC_MYSQL_SSL_CA`.** The engine's own variable takes a CA as
  PEM, turns TLS on, and verifies against that CA instead. Use it to choose
  the CA yourself, or where a connection string cannot carry the
  parameter. PlanetScale's certificates chain to public roots, so the CA
  is the operating system's bundle:

  ```sh
  export WORKOS_SYNC_MYSQL_SSL_CA="$(cat /etc/ssl/cert.pem)"                   # macOS
  export WORKOS_SYNC_MYSQL_SSL_CA="$(cat /etc/ssl/certs/ca-certificates.crt)"  # Debian, Ubuntu
  ```

  A wrong path makes `$(cat ...)` expand to nothing. The command refuses
  a variable that is set but empty; 0.1.0 read it as unset and connected
  without TLS. In embedded code, pass the same text as `sslCa` to
  `MysqlDestination`. When both settings are present, the variable's CA
  is the one used.

With neither, the connection is plaintext and PlanetScale refuses it:
`server does not allow insecure connections, client must use SSL/TLS`.

Either way the certificate has to name the host in the connection string,
not only chain to a trusted root. `mysql2` checks the chain but not the
host name unless it is told to, and 0.1.0 did not tell it: with the
operating system's bundle as the CA, any publicly trusted certificate
passed, for any host. Later releases turn the check on for both settings.
To reach a server through a tunnel, where the host name is not the
server's, say so in the connection string with
`ssl={"rejectUnauthorized":true,"verifyIdentity":false}`.

### Verify the endpoint

The package's database-backed suite cannot run against Vitess: it creates
a database per test, which Vitess has no statement for. Verify with the
command instead, against a development branch or a `main` with safe
migrations off, and a throwaway WorkOS environment:

```sh
npm install @workos/sync-mysql      # npx runs the command this installs

export DATABASE_URL='mysql://USER:PASSWORD@HOST:3306/DATABASE?ssl={"rejectUnauthorized":true}'
export WORKOS_SYNC_SCHEMA=DATABASE
export WORKOS_API_KEY=sk_test_...
export WORKOS_ENVIRONMENT_NAME=staging   # the environment the key belongs to

npx workos-sync-mysql init          # creates the tables and views inside the branch database
npx workos-sync-mysql init          # second run: nothing to apply
npx workos-sync-mysql backfill
npx workos-sync-mysql tick
npx workos-sync-mysql reconcile     # exit 0: every stream compared, nothing drifted
npx workos-sync-mysql status
```

`WORKOS_ENVIRONMENT_NAME` is the label written to every row as `_env`. It
defaults to `production` whatever key is in use, and it is part of every
primary key, so set it before the first write and keep it the same for
every command;
[Getting started](../getting-started.md#create-the-schema) has the detail.

That lifecycle passed against a PS-10 in November 2026, generated columns,
`_live` views, temporary tables and the `GET_LOCK` migration lock included.
That run took its TLS settings from `WORKOS_SYNC_MYSQL_SSL_CA`; PlanetScale
ran the same commands with the `ssl` parameter instead, and each connected
over TLS 1.3 with the chain verified.

`status` only reads. Against a branch with no mirror it reports
`"initialized": false` and creates nothing; on 0.1.0 it applied the schema
first, like every other command, so a status check created the tables.

### Run the sync

```sh
npm install @workos/sync-mysql

export DATABASE_URL='mysql://USER:PASSWORD@HOST:3306/DATABASE?ssl={"rejectUnauthorized":true}'
export WORKOS_SYNC_SCHEMA=DATABASE
export WORKOS_API_KEY=sk_...
export WORKOS_ENVIRONMENT_NAME=production   # the environment the key belongs to

npx workos-sync-mysql init
npx workos-sync-mysql backfill
npx workos-sync-mysql listen
```

If the production branch has safe migrations on, follow the next section
instead of running `init` there.

### Safe migrations

With safe migrations on, PlanetScale refuses direct DDL on the branch.
`init` cannot create the mirror there, and every command but `status` runs
`init` first, so they fail until the schema has been deployed. It goes in
from a development branch, as two deploy requests: the tables, then the
views. One deploy request carrying the whole schema fails PlanetScale's
lint with `TOO_MANY_DEPENDENT_CHANGES`: a deploy request may hold 10
inter-dependent operations, and the 17 `_live` views are 17.

1. Create a development branch from the production branch, and an
   **Admin** password for it.
2. Run `init` against the development branch.
3. On the development branch, drop the `_live` views. For the default
   streams, with no table prefix:

   ```sql
   drop view users_live, organizations_live, organization_memberships_live,
     organization_domains_live, organization_roles_live, invitations_live,
     sessions_live, directories_live, directory_users_live,
     directory_groups_live, directory_group_memberships_live,
     connections_live, groups_live, group_memberships_live, roles_live,
     permissions_live, feature_flags_live;
   ```

4. Open a deploy request, deploy it, and skip the revert period. The
   production branch now has the tables.
5. On the development branch, run `init --repair`, which goes through the
   catalog again, finds the tables in place, and creates the views. On
   0.1.0, where `--repair` stops at the recorded fingerprint like plain
   `init` does, run `delete from _migrations;` and then `init` instead.
   Open a second deploy request, deploy it, and skip the revert period.
6. Against the production branch, with an **Admin** password, run
   `backfill`, then `listen`.

Step 6 changes no schema. The `init` inside `backfill` finds the tables and
views in place, checks the live columns against the catalog through a
temporary table, which is session state rather than a schema change, and
records the fingerprint.

A change to the catalog goes in the same way, from a development branch
through a deploy request: a stream added with `--streams`, or a release of
the package that adds a column. Until it is deployed, every command run
with the new catalog fails on the production branch with PlanetScale's
`direct DDL is disabled`, which does not say what is missing; `init
--print-sql` with the new catalog prints the whole schema to compare
against.

PlanetScale ran these steps with 0.1.0 in November 2026, and `init` on
the production branch afterwards applied no statements. They have not
been run from this repository; the lifecycle under "Verify the endpoint"
ran with safe migrations off.

### Reconcile

`reconcile` stages its comparison in temporary tables, so the password it
runs with needs the **Admin** role, like `init`. With a lesser one every
stream fails with `DDL command denied`.

PlanetScale ends a transaction 20 seconds after it opens, one of its
[system limits](https://planetscale.com/docs/vitess/troubleshooting/planetscale-system-limits).
`@workos/sync-mysql` 0.1.0 held one transaction open for the whole of a
stream's listing from WorkOS, so a stream that lists for longer than that
failed with `exceeded timeout: 20s`. In PlanetScale's testing `sessions`,
which is listed with one request per user, took 28 to 30 seconds at 301
users and failed every time; at 151 users it passed. The streams listed
per organization (`organization_memberships`, `groups`,
`organization_roles`) and per directory (`directory_users`,
`directory_groups`) work the same way.

Later releases hold the transaction only while they capture the mirror's
ids, and list with none open. That has passed against MySQL 8.4 and has
not yet been run against PlanetScale. On 0.1.0, or if a stream still
fails, leave it out with `--exclude-streams`:

```sh
npx workos-sync-mysql reconcile --exclude-streams sessions
```

An excluded stream is still backfilled and tailed; it is only left
unchecked for drift.

A stream that could not be compared is reported with `"complete": false`
and the error in `skippedReason`. Releases after 0.1.0 also mark it
`"failed": true` and exit 1, so the exit code can be trusted as a
scheduled check: 0 for a clean run, 2 for drift, 1 when something could
not be checked. 0.1.0 exits 0 in that case, so read `complete` for each
stream there.

## Branches

A PlanetScale branch is a separate database with its own credentials:

- **Create a role or password per branch.** A Postgres role and a Vitess
  password each connect to one branch only.
- **A new branch has no rows.** A new Postgres branch has no schema or
  data. A new Vitess branch has its parent's schema, so the mirror's
  tables and views are there, empty. On either, run `init` and `backfill`.
- **A branch restored from a backup has the mirror and its cursor.** On
  either engine it carries the rows and `_sync_state` as they were at the
  backup, so `tick` and `listen` continue from the parent's cursor rather
  than starting over.
- **Two branches syncing the same environment are two syncs on one key**,
  and share its [rate limit](../operations.md#rate-limits).
- **Vitess development branches are where schema changes are made.** Run
  `init` on a development branch and deploy the difference to production;
  [Safe migrations](#safe-migrations) above has the steps.

## Query it

Everything in [Querying the mirror](../queries.md) applies. The
console's **Console** entry is a web `psql` or `mysql` session on the
branch, and **Insights** shows query performance. For a production branch
the web console is off until the database setting **Allow web console
access to production branches** is checked. From your own machine,
`psql` takes the URI PlanetScale shows (with `sslrootcert=system`, which
is exactly what libpq wants), and `mysql` takes
`--ssl-mode=VERIFY_IDENTITY` with the host, user and password:

```sql
-- Postgres
select email from workos.users_live;
-- Vitess: the branch database is already selected
select email from users_live;
-- Vitess, with WORKOS_SYNC_TABLE_PREFIX=workos_
select email from workos_users_live;
```
