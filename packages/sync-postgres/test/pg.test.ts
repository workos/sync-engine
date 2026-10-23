import { it, expect } from 'vitest'
import { createTestDb } from './helpers/pg.js'

it('creates isolated databases without carrying over schema or committed data', async () => {
  const first = await createTestDb()
  try {
    await first.exec(`create schema isolation; create table isolation.values (value text)`)
    await first.transaction(async (tx) => {
      await tx.query(`insert into isolation.values values ($1)`, ['first'])
    })

    const second = await createTestDb()
    try {
      expect(second.raw).not.toBe(first.raw)
      const schema = await second.query(`select to_regnamespace('isolation') as name`)
      expect(schema.rows).toEqual([{ name: null }])

      await second.exec(`create schema isolation; create table isolation.values (value text)`)
      await second.query(`insert into isolation.values values ($1)`, ['second'])
      expect((await first.query(`select value from isolation.values`)).rows).toEqual([{ value: 'first' }])
      expect((await second.query(`select value from isolation.values`)).rows).toEqual([{ value: 'second' }])
    } finally {
      await second.close()
    }
  } finally {
    await first.close()
  }
})
