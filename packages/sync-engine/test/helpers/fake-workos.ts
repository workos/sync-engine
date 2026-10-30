/**
 * An in-memory stand-in for the WorkOS API.
 *
 * It implements the parts of the surface the engine depends on, including the
 * behaviours that make syncing hard: cursor pagination with a 100-item cap,
 * event payloads that are strict subsets of the REST objects, out-of-order and
 * duplicate delivery, rate limiting, and deletes that vanish from list
 * endpoints entirely.
 *
 * The same handler serves the unit tests (as a `fetch` implementation) and the
 * end-to-end example (over a real HTTP server).
 */
import { createServer, type Server } from 'node:http'
import type { WorkOSEvent } from '../../src/types.js'

let counter = 0
/** Monotonic, sortable ids in the shape WorkOS uses: prefix + ULID-like suffix. */
export function id(prefix: string, seq?: number): string {
  const n = seq ?? ++counter
  const time = (1_700_000_000_000 + n * 1000).toString(32).toUpperCase().padStart(10, '0')
  return `${prefix}${time}${String(n).padStart(16, '0')}`
}

export function resetIds(): void {
  counter = 0
}

export interface FakeOptions {
  /** Fail this many requests with 429 before succeeding. */
  rateLimitFirst?: number
  /** Serve events in a deliberately shuffled order. */
  shuffleEvents?: boolean
  /** Duplicate every event once, as at-least-once delivery would. */
  duplicateEvents?: boolean
  apiKey?: string
}

export class FakeWorkOS {
  users = new Map<string, Record<string, unknown>>()
  organizations = new Map<string, Record<string, unknown>>()
  memberships = new Map<string, Record<string, unknown>>()
  invitations = new Map<string, Record<string, unknown>>()
  directories = new Map<string, Record<string, unknown>>()
  directoryUsers = new Map<string, Record<string, unknown>>()
  connections = new Map<string, Record<string, unknown>>()
  events: WorkOSEvent[] = []
  requestLog: string[] = []
  /** The User-Agent of every request, in order. */
  userAgents: string[] = []
  private rateLimitRemaining: number

  constructor(private opts: FakeOptions = {}) {
    this.rateLimitRemaining = opts.rateLimitFirst ?? 0
  }

  /**
   * A monotonic clock. Real WorkOS timestamps have millisecond precision, so
   * successive mutations to one object get distinct `updated_at` values; a
   * fake that reused one timestamp would make out-of-order tests unfalsifiable.
   */
  private clock = 0

  private now(offsetMs?: number): string {
    const tick = offsetMs ?? this.clock++
    return new Date(Date.now() - 60_000 + tick).toISOString()
  }

  private emit(event: string, data: Record<string, unknown>, context?: Record<string, unknown>): WorkOSEvent {
    const e: WorkOSEvent = {
      id: id('event_'),
      event,
      data,
      created_at: this.now(),
      ...(context ? { context } : {}),
    }
    this.events.push(e)
    return e
  }

  // ------------------------------------------------------------- factories

  createUser(fields: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
    const uid = (fields['id'] as string) ?? id('user_')
    const user = {
      object: 'user',
      id: uid,
      email: `user${uid.slice(-4)}@example.com`,
      email_verified: true,
      first_name: 'Ada',
      last_name: 'Lovelace',
      profile_picture_url: null,
      // These four are absent from user.* event payloads. They exist only via
      // the REST object, which is exactly the trap the engine has to survive.
      external_id: `ext_${uid.slice(-4)}`,
      metadata: { plan: 'pro' },
      last_sign_in_at: this.now(),
      locale: 'en',
      created_at: this.now(),
      updated_at: this.now(),
      ...fields,
    }
    this.users.set(user['id'] as string, user)
    this.emit('user.created', this.userEventPayload(user))
    return user
  }

  /** The documented subset that `user.*` events actually carry. */
  userEventPayload(user: Record<string, unknown>): Record<string, unknown> {
    return {
      object: 'user',
      id: user['id'],
      email: user['email'],
      email_verified: user['email_verified'],
      first_name: user['first_name'],
      last_name: user['last_name'],
      profile_picture_url: user['profile_picture_url'],
      created_at: user['created_at'],
      updated_at: user['updated_at'],
    }
  }

  updateUser(uid: string, patch: Record<string, unknown>): Record<string, unknown> {
    const user = { ...this.users.get(uid)!, ...patch, updated_at: this.now() }
    this.users.set(uid, user)
    this.emit('user.updated', this.userEventPayload(user))
    return user
  }

  deleteUser(uid: string): void {
    const user = this.users.get(uid)!
    this.users.delete(uid)
    this.emit('user.deleted', this.userEventPayload(user))
  }

  createOrganization(fields: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
    const oid = (fields['id'] as string) ?? id('org_')
    const org = {
      object: 'organization',
      id: oid,
      name: `Org ${oid.slice(-4)}`,
      external_id: null,
      metadata: {},
      stripe_customer_id: `cus_${oid.slice(-6)}`,
      allow_profiles_outside_organization: false,
      domains: [],
      created_at: this.now(),
      updated_at: this.now(),
      ...fields,
    }
    this.organizations.set(oid, org)
    this.emit('organization.created', this.organizationEventPayload(org))
    return org
  }

  /**
   * The documented subset that `organization.*` events carry: no metadata,
   * stripe_customer_id or allow_profiles_outside_organization. `domains` is
   * carried whole, DNS challenge and all.
   */
  organizationEventPayload(org: Record<string, unknown>): Record<string, unknown> {
    return {
      object: 'organization',
      id: org['id'],
      name: org['name'],
      external_id: org['external_id'],
      domains: org['domains'],
      created_at: org['created_at'],
      updated_at: org['updated_at'],
    }
  }

  /**
   * Add a domain to an organization, as the dashboard or the organization
   * update endpoint would: the domain object is carried inline on the
   * organization, `organization_domain.created` fires, and so does
   * `organization.updated` with the new `domains`. The object carries the DNS
   * challenge, and unlike an invitation's token so does every event about
   * it; the engine has to strip it from both.
   */
  createDomain(orgId: string, domain: string, fields: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
    const did = (fields['id'] as string) ?? id('org_domain_')
    const d = {
      object: 'organization_domain',
      id: did,
      organization_id: orgId,
      domain,
      state: 'pending',
      verification_strategy: 'dns',
      verification_token: `vt_${did.slice(-8)}`,
      verification_prefix: `superapp-domain-verification-${did.slice(-6).toLowerCase()}`,
      created_at: this.now(),
      updated_at: this.now(),
      ...fields,
    }
    this.setDomain(d)
    this.emit('organization_domain.created', d)
    this.emit('organization.updated', this.organizationEventPayload(this.organizations.get(orgId)!))
    return d
  }

  verifyDomain(domainId: string): Record<string, unknown> {
    const d = { ...this.domain(domainId), state: 'verified', updated_at: this.now() }
    this.setDomain(d)
    this.emit('organization_domain.verified', d)
    return d
  }

  /** This event wraps the object, with the reason beside it, and leaves out the prefix. */
  failDomainVerification(domainId: string, reason = 'domain_verification_period_expired'): Record<string, unknown> {
    const d: Record<string, unknown> = { ...this.domain(domainId), state: 'failed', updated_at: this.now() }
    this.setDomain(d)
    const { verification_prefix: _prefix, ...payload } = d
    this.emit('organization_domain.verification_failed', { reason, organization_domain: payload })
    return d
  }

  private domain(domainId: string): Record<string, unknown> {
    for (const org of this.organizations.values()) {
      const found = ((org['domains'] as Record<string, unknown>[] | undefined) ?? []).find((d) => d['id'] === domainId)
      if (found) return found
    }
    throw new Error(`no such domain: ${domainId}`)
  }

  /** Write a domain into its organization's `domains`, replacing any entry with the same id. */
  private setDomain(d: Record<string, unknown>): void {
    const orgId = d['organization_id'] as string
    const org = this.organizations.get(orgId)
    if (!org) throw new Error(`no such organization: ${orgId}`)
    const others = ((org['domains'] as Record<string, unknown>[] | undefined) ?? []).filter((x) => x['id'] !== d['id'])
    this.organizations.set(orgId, { ...org, domains: [...others, d], updated_at: this.now() })
  }

  createMembership(userId: string, orgId: string, role = 'member'): Record<string, unknown> {
    const mid = id('om_')
    const m = {
      object: 'organization_membership',
      id: mid,
      user_id: userId,
      organization_id: orgId,
      status: 'active',
      role: { slug: role },
      created_at: this.now(),
      updated_at: this.now(),
    }
    this.memberships.set(mid, m)
    this.emit('organization_membership.created', m)
    return m
  }

  createInvitation(orgId: string, fields: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
    const iid = (fields['id'] as string) ?? id('invitation_')
    const token = `tok_${iid.slice(-8)}`
    const inv = {
      object: 'invitation',
      id: iid,
      email: `invitee${iid.slice(-4)}@example.com`,
      state: 'pending',
      accepted_at: null,
      revoked_at: null,
      expires_at: new Date(Date.now() + 7 * 86_400_000).toISOString(),
      organization_id: orgId,
      inviter_user_id: null,
      accepted_user_id: null,
      role_slug: 'member',
      // The live acceptance credential. The REST object carries it, in both
      // the list and get-by-id responses; `invitation.*` events never do.
      token,
      accept_invitation_url: `https://auth.example.com/invite?invitation_token=${token}`,
      created_at: this.now(),
      updated_at: this.now(),
      ...fields,
    }
    this.invitations.set(iid, inv)
    this.emit('invitation.created', this.invitationEventPayload(inv))
    return inv
  }

  /** The documented subset that `invitation.*` events carry: no token, no URL. */
  invitationEventPayload(inv: Record<string, unknown>): Record<string, unknown> {
    const { token: _token, accept_invitation_url: _url, ...rest } = inv
    return rest
  }

  acceptInvitation(iid: string, userId: string): Record<string, unknown> {
    const inv = {
      ...this.invitations.get(iid)!,
      state: 'accepted',
      accepted_at: this.now(),
      accepted_user_id: userId,
      updated_at: this.now(),
    }
    this.invitations.set(iid, inv)
    this.emit('invitation.accepted', this.invitationEventPayload(inv))
    return inv
  }

  createDirectory(orgId: string): Record<string, unknown> {
    const did = id('directory_')
    const d = {
      object: 'directory',
      id: did,
      organization_id: orgId,
      name: 'Acme SCIM',
      domain: 'acme.com',
      type: 'okta scim v2.0',
      state: 'linked',
      external_key: 'abc123',
      created_at: this.now(),
      updated_at: this.now(),
    }
    this.directories.set(did, d)
    this.emit('dsync.activated', d)
    return d
  }

  createDirectoryUser(directoryId: string, orgId: string): Record<string, unknown> {
    const duid = id('directory_user_')
    const du = {
      object: 'directory_user',
      id: duid,
      directory_id: directoryId,
      organization_id: orgId,
      idp_id: `idp_${duid.slice(-4)}`,
      email: `scim${duid.slice(-4)}@acme.com`,
      first_name: 'Grace',
      last_name: 'Hopper',
      state: 'active',
      job_title: 'Engineer',
      created_at: this.now(),
      updated_at: this.now(),
    }
    this.directoryUsers.set(duid, du)
    this.emit('dsync.user.created', du)
    return du
  }

  /** Sign-in events: no object, no REST endpoint, only ever visible here. */
  emitAuthAttempt(userId: string, ok: boolean, type = 'password'): WorkOSEvent {
    return this.emit(`authentication.${type}_${ok ? 'succeeded' : 'failed'}`, {
      user_id: userId,
      email: (this.users.get(userId)?.['email'] as string) ?? 'unknown@example.com',
      type,
      ip_address: '203.0.113.7',
      user_agent: 'Mozilla/5.0',
      ...(ok ? {} : { error: { code: 'invalid_credentials', message: 'Invalid password' } }),
    })
  }

  /** An event type this build of the engine has never seen. */
  emitUnknown(): WorkOSEvent {
    return this.emit('quantum_widget.entangled', { id: id('qw_'), spin: 'up' })
  }

  // -------------------------------------------------------------- handler

  async handle(rawUrl: string, init?: { headers?: Record<string, string> }): Promise<Response> {
    const url = new URL(rawUrl, 'http://fake.workos.local')
    this.requestLog.push(url.pathname + url.search)
    this.userAgents.push(init?.headers?.['User-Agent'] ?? init?.headers?.['user-agent'] ?? '')

    if (this.opts.apiKey) {
      const auth = init?.headers?.['Authorization'] ?? init?.headers?.['authorization']
      if (auth !== `Bearer ${this.opts.apiKey}`) {
        return json({ message: 'Unauthorized' }, 401)
      }
    }

    if (this.rateLimitRemaining > 0) {
      this.rateLimitRemaining--
      // Some WorkOS endpoints return a bare JSON string here, not an object —
      // the client must not try to parse it for control flow.
      return new Response('"Ratelimited"', {
        status: 429,
        headers: { 'retry-after': '0', 'content-type': 'application/json' },
      })
    }

    const limit = Math.min(Number(url.searchParams.get('limit') ?? 10), 100)
    const after = url.searchParams.get('after')
    const path = url.pathname

    if (path === '/events') return this.handleEvents(url, limit, after)

    const listOf = (map: Map<string, Record<string, unknown>>, filter?: (r: Record<string, unknown>) => boolean) => {
      let rows = [...map.values()]
      if (filter) rows = rows.filter(filter)
      rows.sort((a, b) => String(a['id']).localeCompare(String(b['id'])))
      return paginate(rows, limit, after)
    }

    if (path === '/user_management/users') return json(listOf(this.users))
    if (path === '/organizations') return json(listOf(this.organizations))
    if (path === '/user_management/invitations') return json(listOf(this.invitations))
    if (path === '/directories') return json(listOf(this.directories))
    if (path === '/connections') return json(listOf(this.connections))
    if (path === '/user_management/organization_memberships') {
      const org = url.searchParams.get('organization_id')
      const user = url.searchParams.get('user_id')
      if (!org && !user) {
        // The real endpoint requires one of these filters.
        return json({ errors: [{ code: 'required', field: 'organization_id' }] }, 422)
      }
      return json(
        listOf(this.memberships, (m) =>
          org ? m['organization_id'] === org : m['user_id'] === user,
        ),
      )
    }
    if (path === '/directory_users') {
      const dir = url.searchParams.get('directory')
      return json(listOf(this.directoryUsers, (d) => !dir || d['directory_id'] === dir))
    }
    if (path === '/directory_groups') return json({ data: [], list_metadata: { after: null } })
    if (path === '/authorization/roles' || path === '/authorization/permissions') {
      // Roles come back unpaginated, with no list_metadata at all.
      return json({ object: 'list', data: [] })
    }
    if (path === '/feature-flags') return json({ data: [], list_metadata: { after: null } })
    if (path === '/webhook_endpoints') return json({ data: [], list_metadata: { after: null } })
    if (path === '/authorization/resources') return json({ data: [], list_metadata: { after: null } })

    // Get-by-id, used for hydration. A 404 here is a delete signal.
    const userGet = /^\/user_management\/users\/([^/]+)$/.exec(path)
    if (userGet) {
      const u = this.users.get(userGet[1]!)
      return u ? json(u) : json({ message: 'not found' }, 404)
    }
    const orgGet = /^\/organizations\/([^/]+)$/.exec(path)
    if (orgGet) {
      const o = this.organizations.get(orgGet[1]!)
      return o ? json(o) : json({ message: 'not found' }, 404)
    }
    const sessionsGet = /^\/user_management\/users\/([^/]+)\/sessions$/.exec(path)
    if (sessionsGet) return json({ data: [], list_metadata: { after: null } })
    const orgGroups = /^\/organizations\/([^/]+)\/groups$/.exec(path)
    if (orgGroups) return json({ data: [], list_metadata: { after: null } })
    const orgRoles = /^\/authorization\/organizations\/([^/]+)\/roles$/.exec(path)
    if (orgRoles) return json({ object: 'list', data: [] })

    return json({ message: `no fake route for ${path}` }, 404)
  }

  private handleEvents(url: URL, limit: number, after: string | null): Response {
    const order = url.searchParams.get('order') ?? 'desc'
    const filter = url.searchParams.get('events')?.split(',')

    // The live endpoint requires this filter and rejects the request without
    // it. The published OpenAPI spec once marked it optional and now marks it
    // required. Enforcing it here is what keeps the test suite honest about
    // the real contract.
    if (!filter || filter.length === 0) {
      return json(
        {
          message:
            'One or more event names (e.g. dsync.user.created) must be provided using the events parameter.',
          code: 'invalid_events_request',
        },
        400,
      )
    }

    let rows = [...this.events]
    if (filter) rows = rows.filter((e) => filter.includes(e.event))
    rows.sort((a, b) => a.id.localeCompare(b.id))

    if (order === 'desc') {
      rows = rows.slice().reverse()
      const page = rows.slice(0, limit)
      return json({ data: page, list_metadata: { after: page.at(-1)?.id ?? null } })
    }

    if (after) {
      const idx = rows.findIndex((e) => e.id === after)
      rows = idx >= 0 ? rows.slice(idx + 1) : rows
    }

    let page = rows.slice(0, limit)
    if (this.opts.duplicateEvents) page = page.flatMap((e) => [e, e])
    if (this.opts.shuffleEvents) page = page.slice().reverse()

    const hasMore = rows.length > limit
    return json({
      data: page,
      list_metadata: { after: hasMore ? (rows[limit - 1]?.id ?? null) : null },
    })
  }

  /** A `fetch` implementation for injecting into WorkOSClient. */
  get fetchImpl(): typeof fetch {
    return ((input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString()
      return this.handle(url, { headers: (init?.headers as Record<string, string>) ?? {} })
    }) as typeof fetch
  }

  /** Serve over real HTTP, for the end-to-end example. */
  listen(port = 0): Promise<{ server: Server; url: string }> {
    return new Promise((resolve) => {
      const server = createServer(async (req, res) => {
        const response = await this.handle(req.url ?? '/', {
          headers: req.headers as Record<string, string>,
        })
        const body = await response.text()
        res.writeHead(response.status, { 'content-type': 'application/json' })
        res.end(body)
      })
      server.listen(port, () => {
        const addr = server.address()
        const p = typeof addr === 'object' && addr ? addr.port : port
        resolve({ server, url: `http://127.0.0.1:${p}` })
      })
    })
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

function paginate(rows: Record<string, unknown>[], limit: number, after: string | null) {
  let start = 0
  if (after) {
    const idx = rows.findIndex((r) => r['id'] === after)
    start = idx >= 0 ? idx + 1 : 0
  }
  const page = rows.slice(start, start + limit)
  const hasMore = start + limit < rows.length
  return {
    data: page,
    list_metadata: { after: hasMore ? (page.at(-1)?.['id'] ?? null) : null },
  }
}
