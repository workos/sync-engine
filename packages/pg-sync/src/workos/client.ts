/**
 * WorkOS API client: pagination, rate limiting and retries.
 *
 * Rate limits that actually bind, per the WorkOS reference:
 *   - 6,000 requests / 60s per API key, globally
 *   - 1,000 reads / 10s on /user_management/*
 *   - **4 requests/second per directory** on /directory_users
 *
 * The directory limit is per directory rather than per key, so it does not
 * aggregate: backfilling twenty directories does not get you twenty times the
 * throughput on any one of them. Buckets are therefore keyed by scope.
 */
import type { Logger } from '../types.js'

export interface ListPage<T = Record<string, unknown>> {
  data: T[]
  listMetadata: { after: string | null; before?: string | null }
}

export class WorkOSApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly body: string,
  ) {
    super(message)
    this.name = 'WorkOSApiError'
  }
}

/** Continuous-refill token bucket. */
export class TokenBucket {
  private tokens: number
  private last: number

  constructor(
    private capacity: number,
    private refillPerSecond: number,
    private now: () => number = Date.now,
  ) {
    this.tokens = capacity
    this.last = now()
  }

  /** ms to wait before a token is available; 0 if one is free now. */
  take(): number {
    const t = this.now()
    const elapsed = (t - this.last) / 1000
    this.last = t
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerSecond)
    if (this.tokens >= 1) {
      this.tokens -= 1
      return 0
    }
    const deficit = 1 - this.tokens
    this.tokens -= 1
    return Math.ceil((deficit / this.refillPerSecond) * 1000)
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export interface WorkOSClientOptions {
  apiKey: string
  apiUrl?: string
  logger?: Logger
  fetchImpl?: typeof fetch
  maxRetries?: number
  /** Page size. The API default is 10; we always ask for the maximum. */
  pageSize?: number
}

export class WorkOSClient {
  private apiUrl: string
  private buckets = new Map<string, TokenBucket>()
  private fetchImpl: typeof fetch
  readonly pageSize: number

  constructor(private opts: WorkOSClientOptions) {
    this.apiUrl = (opts.apiUrl ?? 'https://api.workos.com').replace(/\/$/, '')
    this.fetchImpl = opts.fetchImpl ?? fetch
    this.pageSize = opts.pageSize ?? 100
  }

  private bucketFor(key: string): TokenBucket {
    let b = this.buckets.get(key)
    if (!b) {
      // 4 req/s per directory; a conservative shared budget elsewhere so that
      // backfill leaves headroom for the application's own API traffic.
      b = key.startsWith('directory:')
        ? new TokenBucket(4, 4)
        : key === 'user_management'
          ? new TokenBucket(50, 50)
          : new TokenBucket(50, 50)
      this.buckets.set(key, b)
    }
    return b
  }

  async request<T>(path: string, params?: Record<string, string>, rateKey = 'global'): Promise<T> {
    const wait = this.bucketFor(rateKey).take()
    if (wait > 0) await sleep(wait)

    const url = new URL(this.apiUrl + path)
    for (const [k, v] of Object.entries(params ?? {})) {
      if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v)
    }

    const maxRetries = this.opts.maxRetries ?? 5
    let attempt = 0

    for (;;) {
      let res: Response
      try {
        res = await this.fetchImpl(url.toString(), {
          headers: {
            Authorization: `Bearer ${this.opts.apiKey}`,
            'User-Agent': 'workos-pg-sync',
            Accept: 'application/json',
          },
        })
      } catch (err) {
        if (attempt >= maxRetries) throw err
        const backoff = Math.min(30_000, 2 ** attempt * 250)
        this.opts.logger?.warn('network error, retrying', { path, attempt, backoff })
        await sleep(backoff)
        attempt++
        continue
      }

      if (res.ok) return (await res.json()) as T

      const body = await res.text().catch(() => '')

      // 429s and 5xx are transient. Honour Retry-After when present; the
      // 429 body on some endpoints is a bare JSON string, not an error object,
      // so never try to parse it for control flow.
      if ((res.status === 429 || res.status >= 500) && attempt < maxRetries) {
        const retryAfter = Number(res.headers.get('retry-after'))
        const backoff = Number.isFinite(retryAfter) && retryAfter > 0
          ? retryAfter * 1000
          : Math.min(30_000, 2 ** attempt * 500)
        this.opts.logger?.warn('rate limited or server error, retrying', {
          path,
          status: res.status,
          attempt,
          backoff,
        })
        await sleep(backoff)
        attempt++
        continue
      }

      throw new WorkOSApiError(`${res.status} on ${path}: ${body.slice(0, 200)}`, res.status, body)
    }
  }

  /** One page of a list endpoint. Handles both envelope shapes. */
  async list<T = Record<string, unknown>>(
    path: string,
    params: Record<string, string> = {},
    rateKey = 'global',
    shape: 'list' | 'array' = 'list',
  ): Promise<ListPage<T>> {
    const res = await this.request<unknown>(
      path,
      { limit: String(this.pageSize), ...params },
      rateKey,
    )
    // A few endpoints (identities) return a bare array, and the roles
    // endpoints return {object:'list', data:[]} with no list_metadata at all.
    if (Array.isArray(res)) return { data: res as T[], listMetadata: { after: null } }
    const obj = res as { data?: T[]; list_metadata?: { after?: string | null } }
    return {
      data: obj.data ?? [],
      listMetadata: { after: obj.list_metadata?.after ?? null },
    }
  }

  /** Every page of a list endpoint. */
  async *paginate<T = Record<string, unknown>>(
    path: string,
    params: Record<string, string> = {},
    rateKey = 'global',
    shape: 'list' | 'array' = 'list',
    startAfter?: string | null,
  ): AsyncGenerator<{ rows: T[]; after: string | null }> {
    let after: string | null = startAfter ?? null
    for (;;) {
      const page: ListPage<T> = await this.list<T>(
        path,
        after ? { ...params, after } : params,
        rateKey,
        shape,
      )
      yield { rows: page.data, after: page.listMetadata.after }
      if (!page.listMetadata.after || page.data.length === 0) return
      after = page.listMetadata.after
    }
  }

  /** Get by id. Returns null on 404, which is how a missed delete is detected. */
  async get<T>(path: string, rateKey = 'global'): Promise<T | null> {
    try {
      return await this.request<T>(path, undefined, rateKey)
    } catch (err) {
      if (err instanceof WorkOSApiError && err.status === 404) return null
      throw err
    }
  }
}
