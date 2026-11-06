/** Runs in every test file: gives the shared client back once the file is done. */
import { afterAll } from 'vitest'
import { closeSharedClient } from './helpers/mongo.js'

afterAll(() => closeSharedClient())
