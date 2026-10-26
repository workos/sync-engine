# Database tests

`pnpm test` runs the PGlite-backed suite without a database service.
Multi-connection regressions also run when `TEST_DATABASE_URL` is set:

```sh
TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres pnpm test
```

Use a disposable PostgreSQL database with permission to create schemas. Each
integration test creates a uniquely named schema and drops it on completion.
CI supplies PostgreSQL 17 and runs these tests on both supported Node versions.
