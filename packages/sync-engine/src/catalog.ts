/**
 * The stream catalog: every WorkOS object the engine mirrors, how to backfill
 * it, and which events drive it.
 *
 * Two facts from the WorkOS API shape the definitions here:
 *
 * 1. Most event payloads are the full REST object, but not every one is:
 *    deprecated fields are left out of event payloads, and `role.*` events
 *    have no `id` at all. A whole-row replace would null every column an
 *    event leaves out, so event writes merge into the row, and backfill,
 *    reconcile and hydration, which write the REST object, replace it. See
 *    `db/upsert.ts`.
 *
 * 2. Several objects have events but no usable list endpoint, and several have
 *    list endpoints but no events. Both are represented honestly: a stream with
 *    no `list` cannot be backfilled or reconciled, and a `poll` stream has no
 *    events and is only as fresh as its last snapshot.
 */
import type { ColumnDef, StreamDef } from './types.js'
import type { components } from '@workos/openapi-spec'

/**
 * Every event type the published OpenAPI spec declares, as a literal union.
 * `EventSchema` is a `oneOf` discriminated on `event`, so this is the spec's
 * own list rather than a hand-maintained copy. It exists only to typecheck the
 * catalog below: a name WorkOS retires fails `pnpm typecheck` on the next spec
 * bump instead of turning into `400 invalid_events_request` in production.
 *
 * It is deliberately not part of `StreamDef`. `@workos/openapi-spec` is a
 * devDependency, and putting the union in the public types would make every
 * consumer install it to typecheck.
 */
type EventName = components['schemas']['EventSchema']['event']

/** `StreamDef` with its event names narrowed to the spec's union. */
type CatalogStreamDef = Omit<StreamDef, 'events'> & {
  events?: { upsert?: EventName[]; delete?: EventName[] }
}

const timestamps = [
  { name: 'created_at', type: 'timestamptz' as const, path: 'created_at' },
  { name: 'updated_at', type: 'timestamptz' as const, path: 'updated_at' },
]

export const STREAMS: StreamDef[] = [
  // ------------------------------------------------------------- identity
  {
    name: 'users',
    description: 'AuthKit users',
    mode: 'mirror',
    group: 'identity',
    key: ['id'],
    prefixes: ['user_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'email', type: 'text', path: 'email', index: true },
      { name: 'email_verified', type: 'boolean', path: 'email_verified' },
      { name: 'first_name', type: 'text', path: 'first_name' },
      { name: 'last_name', type: 'text', path: 'last_name' },
      { name: 'profile_picture_url', type: 'text', path: 'profile_picture_url' },
      { name: 'external_id', type: 'text', path: 'external_id', index: true },
      { name: 'metadata', type: 'jsonb', path: 'metadata' },
      { name: 'last_sign_in_at', type: 'timestamptz', path: 'last_sign_in_at' },
      { name: 'locale', type: 'text', path: 'locale' },
      ...timestamps,
    ],
    events: { upsert: ['user.created', 'user.updated'], delete: ['user.deleted'] },
    list: { path: '/user_management/users', rateKey: 'user_management' },
    get: (id) => `/user_management/users/${id}`,
  },
  {
    name: 'organizations',
    description: 'Organizations',
    mode: 'mirror',
    group: 'identity',
    key: ['id'],
    prefixes: ['org_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'name', type: 'text', path: 'name', index: true },
      { name: 'external_id', type: 'text', path: 'external_id', index: true },
      { name: 'metadata', type: 'jsonb', path: 'metadata' },
      // Billing customer id: the join key to a billing mirror, if you run one.
      { name: 'stripe_customer_id', type: 'text', path: 'stripe_customer_id' },
      // Deprecated upstream and, like every deprecated field, left out of
      // events; backfill, refresh and hydration populate it from the REST object.
      {
        name: 'allow_profiles_outside_organization',
        type: 'boolean',
        path: 'allow_profiles_outside_organization',
      },
      { name: 'domains', type: 'jsonb', path: 'domains' },
      ...timestamps,
    ],
    events: {
      upsert: ['organization.created', 'organization.updated'],
      delete: ['organization.deleted'],
    },
    list: { path: '/organizations' },
    get: (id) => `/organizations/${id}`,
    // Each entry of `domains` is the organization domain object, DNS challenge
    // included (see `organization_domains`). Stripped from this embedded copy
    // too, in the REST object and in `organization.*` events alike, so neither
    // `_raw` nor the `domains` column ever carries it.
    redact: [
      ['domains', 'verification_token'],
      ['domains', 'verification_prefix'],
    ],
  },
  {
    name: 'organization_memberships',
    description: 'User ↔ organization memberships',
    mode: 'mirror',
    group: 'identity',
    key: ['id'],
    prefixes: ['om_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'user_id', type: 'text', path: 'user_id', index: true, references: 'users' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'status', type: 'text', path: 'status' },
      { name: 'role_slug', type: 'text', path: ['role', 'slug'] },
      { name: 'organization_name', type: 'text', path: 'organization_name' },
      { name: 'directory_managed', type: 'boolean', path: 'directory_managed' },
      ...timestamps,
    ],
    events: {
      upsert: ['organization_membership.created', 'organization_membership.updated'],
      delete: ['organization_membership.deleted'],
    },
    // The list endpoint requires a user_id or organization_id filter, and
    // defaults to active-only, so we iterate organizations and ask for every status.
    list: {
      path: '/user_management/organization_memberships',
      rateKey: 'user_management',
      params: { statuses: 'active,inactive,pending' },
      parent: { stream: 'organizations', param: 'organization_id' },
    },
  },
  {
    name: 'organization_domains',
    description: 'Organization domains and their verification state',
    mode: 'mirror',
    group: 'identity',
    key: ['id'],
    prefixes: ['org_domain_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'domain', type: 'text', path: 'domain', index: true },
      { name: 'state', type: 'text', path: 'state' },
      { name: 'verification_strategy', type: 'text', path: 'verification_strategy' },
      ...timestamps,
    ],
    events: {
      upsert: [
        'organization_domain.created',
        'organization_domain.updated',
        'organization_domain.verified',
        'organization_domain.verification_failed',
      ],
      delete: ['organization_domain.deleted'],
    },
    // No standalone list endpoint exists; domains arrive embedded in the
    // organization payload. `backfill` explodes them out of organizations.
    unwrap: (data) => (data['organization_domain'] as Record<string, unknown>) ?? data,
    // The object carries the DNS challenge a pending domain is verified by:
    // the TXT record value (`verification_token`) and its name
    // (`verification_prefix`). Unlike an invitation's token, WorkOS puts it in
    // every `organization_domain.*` event too, so it is stripped from the
    // archive as well as from the row.
    redact: ['verification_token', 'verification_prefix'],
  },
  {
    name: 'invitations',
    description: 'Organization invitations',
    mode: 'mirror',
    group: 'identity',
    key: ['id'],
    prefixes: ['invitation_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'email', type: 'text', path: 'email', index: true },
      { name: 'state', type: 'text', path: 'state' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'inviter_user_id', type: 'text', path: 'inviter_user_id', references: 'users' },
      { name: 'accepted_user_id', type: 'text', path: 'accepted_user_id', references: 'users' },
      { name: 'role_slug', type: 'text', path: ['role', 'slug'] },
      { name: 'expires_at', type: 'timestamptz', path: 'expires_at' },
      { name: 'accepted_at', type: 'timestamptz', path: 'accepted_at' },
      { name: 'revoked_at', type: 'timestamptz', path: 'revoked_at' },
      ...timestamps,
    ],
    events: {
      upsert: ['invitation.created', 'invitation.accepted', 'invitation.resent', 'invitation.revoked'],
    },
    list: { path: '/user_management/invitations', rateKey: 'user_management' },
    // The REST invitation object carries the live acceptance credential
    // (`token`) and a URL that embeds the same token. Anyone who can accept the
    // invite can join the organization as the invited identity, so these must
    // never land in the customer-readable mirror. Event payloads already omit
    // them; this covers the backfill/reconcile/hydrate REST paths.
    redact: ['token', 'accept_invitation_url'],
  },
  {
    name: 'events',
    description: 'Raw event archive — every event, including ones with no mirror table',
    mode: 'append',
    group: 'logs',
    key: ['id'],
    prefixes: ['event_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'event', type: 'text', path: 'event', index: true },
      { name: 'data', type: 'jsonb', path: 'data' },
      { name: 'context', type: 'jsonb', path: 'context' },
      { name: 'object_id', type: 'text', path: ['data', 'id'], index: true },
      { name: 'created_at', type: 'timestamptz', path: 'created_at', index: true },
    ],
  },
  {
    name: 'auth_attempts',
    description:
      'Authentication attempts. These events have no REST endpoint — if you do not capture them they are gone after 90 days',
    mode: 'append',
    group: 'logs',
    key: ['event_id'],
    // Declared here so the subscription list is derived from the catalog. The
    // router reshapes these into log rows rather than upserting an object.
    events: {
      upsert: [
        'authentication.email_verification_succeeded',
        'authentication.email_verification_failed',
        'authentication.magic_auth_succeeded',
        'authentication.magic_auth_failed',
        'authentication.oauth_succeeded',
        'authentication.oauth_failed',
        'authentication.password_succeeded',
        'authentication.password_failed',
        'authentication.passkey_succeeded',
        'authentication.passkey_failed',
        'authentication.mfa_succeeded',
        'authentication.mfa_failed',
        'authentication.sso_succeeded',
        'authentication.sso_failed',
        'authentication.radar_risk_detected',
      ],
    },
    columns: [
      { name: 'event_id', type: 'text', path: 'event_id' },
      { name: 'event', type: 'text', path: 'event', index: true },
      { name: 'type', type: 'text', path: 'type' },
      { name: 'status', type: 'text', path: 'status' },
      { name: 'user_id', type: 'text', path: 'user_id', index: true, references: 'users' },
      { name: 'email', type: 'text', path: 'email', index: true },
      { name: 'ip_address', type: 'text', path: 'ip_address' },
      { name: 'user_agent', type: 'text', path: 'user_agent' },
      { name: 'error_code', type: 'text', path: ['error', 'code'] },
      { name: 'error_message', type: 'text', path: ['error', 'message'] },
      { name: 'created_at', type: 'timestamptz', path: 'created_at', index: true },
    ],
  },

  // ------------------------------------------------ directory sync and SSO
  {
    name: 'sessions',
    description: 'AuthKit sessions',
    mode: 'mirror',
    group: 'identity',
    key: ['id'],
    prefixes: ['session_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'user_id', type: 'text', path: 'user_id', index: true, references: 'users' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'status', type: 'text', path: 'status' },
      { name: 'auth_method', type: 'text', path: 'auth_method' },
      { name: 'ip_address', type: 'text', path: 'ip_address' },
      { name: 'user_agent', type: 'text', path: 'user_agent' },
      { name: 'impersonator_email', type: 'text', path: ['impersonator', 'email'] },
      { name: 'expires_at', type: 'timestamptz', path: 'expires_at' },
      { name: 'ended_at', type: 'timestamptz', path: 'ended_at' },
      ...timestamps,
    ],
    // session.revoked updates status rather than removing the row.
    events: { upsert: ['session.created', 'session.revoked'] },
    // Sessions are only listable per user, and only active ones. Backfill is
    // therefore partial by construction — the stream is really event-sourced.
    list: {
      path: '/user_management/users/{user_id}/sessions',
      rateKey: 'user_management',
      parent: { stream: 'users', param: 'user_id', from: 'path' },
    },
  },
  {
    name: 'directories',
    description: 'Directory Sync connections',
    mode: 'mirror',
    group: 'directory_sync',
    key: ['id'],
    prefixes: ['directory_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'domain', type: 'text', path: 'domain' },
      { name: 'type', type: 'text', path: 'type' },
      { name: 'state', type: 'text', path: 'state', index: true },
      { name: 'external_key', type: 'text', path: 'external_key' },
      ...timestamps,
    ],
    // There is no dsync.updated event: a directory going linked ->
    // invalid_credentials emits nothing. Reconcile is the only way to see it.
    events: { upsert: ['dsync.activated'], delete: ['dsync.deleted'] },
    list: { path: '/directories' },
  },
  {
    name: 'directory_users',
    description: 'Users provisioned over SCIM',
    mode: 'mirror',
    group: 'directory_sync',
    key: ['id'],
    prefixes: ['directory_user_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'directory_id', type: 'text', path: 'directory_id', index: true },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'idp_id', type: 'text', path: 'idp_id' },
      { name: 'email', type: 'text', path: 'email', index: true },
      { name: 'first_name', type: 'text', path: 'first_name' },
      { name: 'last_name', type: 'text', path: 'last_name' },
      { name: 'job_title', type: 'text', path: 'job_title' },
      { name: 'username', type: 'text', path: 'username' },
      { name: 'state', type: 'text', path: 'state', index: true },
      { name: 'role_slug', type: 'text', path: ['role', 'slug'] },
      { name: 'custom_attributes', type: 'jsonb', path: 'custom_attributes' },
      { name: 'raw_attributes', type: 'jsonb', path: 'raw_attributes' },
      ...timestamps,
    ],
    events: {
      upsert: ['dsync.user.created', 'dsync.user.updated'],
      delete: ['dsync.user.deleted'],
    },
    // 4 req/s per directory — the binding constraint on backfill.
    list: {
      path: '/directory_users',
      rateKey: 'directory',
      parent: { stream: 'directories', param: 'directory' },
    },
  },
  {
    name: 'directory_groups',
    description: 'Groups provisioned over SCIM',
    mode: 'mirror',
    group: 'directory_sync',
    key: ['id'],
    prefixes: ['directory_group_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'directory_id', type: 'text', path: 'directory_id', index: true },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'idp_id', type: 'text', path: 'idp_id' },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'raw_attributes', type: 'jsonb', path: 'raw_attributes' },
      ...timestamps,
    ],
    events: {
      upsert: ['dsync.group.created', 'dsync.group.updated'],
      delete: ['dsync.group.deleted'],
    },
    list: {
      path: '/directory_groups',
      rateKey: 'directory',
      parent: { stream: 'directories', param: 'directory' },
    },
  },
  {
    name: 'directory_group_memberships',
    description: 'Directory group membership, from group.user_added / user_removed events',
    mode: 'mirror',
    group: 'directory_sync',
    // The membership itself has no ID upstream; the pair is the key.
    key: ['directory_group_id', 'directory_user_id'],
    columns: [
      { name: 'directory_group_id', type: 'text', path: 'directory_group_id', index: true },
      { name: 'directory_user_id', type: 'text', path: 'directory_user_id', index: true },
      { name: 'directory_id', type: 'text', path: 'directory_id' },
    ],
    // These events carry {directory_id, user, group} rather than a membership
    // object, so the router reshapes them into the pair above before writing.
    // There is no list endpoint for memberships; they are reconstructed by
    // listing directory users per group, which reconcile does not attempt.
    events: {
      upsert: ['dsync.group.user_added'],
      delete: ['dsync.group.user_removed'],
    },
  },
  {
    name: 'connections',
    description: 'SSO connections',
    mode: 'mirror',
    group: 'sso',
    key: ['id'],
    // Longest-prefix-first matters: conn_ is a proper prefix of conn_app_.
    prefixes: ['conn_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'connection_type', type: 'text', path: 'connection_type', index: true },
      { name: 'state', type: 'text', path: 'state', index: true },
      { name: 'domains', type: 'jsonb', path: 'domains' },
      ...timestamps,
    ],
    events: {
      upsert: ['connection.activated', 'connection.deactivated'],
      delete: ['connection.deleted'],
    },
    list: { path: '/connections' },
  },

  // -------------------------------------------------------- authorization
  {
    name: 'groups',
    description: 'AuthKit groups',
    mode: 'mirror',
    group: 'authorization',
    key: ['id'],
    prefixes: ['group_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'description', type: 'text', path: 'description' },
      ...timestamps,
    ],
    events: { upsert: ['group.created', 'group.updated'], delete: ['group.deleted'] },
    list: {
      path: '/organizations/{organization_id}/groups',
      parent: { stream: 'organizations', param: 'organization_id', from: 'path' },
    },
  },
  {
    name: 'group_memberships',
    description: 'Group membership, from group.member_added / member_removed events',
    mode: 'mirror',
    group: 'authorization',
    key: ['group_id', 'organization_membership_id'],
    columns: [
      { name: 'group_id', type: 'text', path: 'group_id', index: true },
      {
        name: 'organization_membership_id',
        type: 'text',
        path: 'organization_membership_id',
        index: true,
      },
    ],
    // The payload is a pair of ids and nothing else, so the router builds the
    // row; the declaration here is what makes the stream's source explicit.
    events: {
      upsert: ['group.member_added'],
      delete: ['group.member_removed'],
    },
  },
  {
    name: 'roles',
    description: 'Environment roles',
    mode: 'mirror',
    group: 'authorization',
    // role.* events carry no id at all — only a slug.
    key: ['slug'],
    columns: [
      { name: 'slug', type: 'text', path: 'slug' },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'description', type: 'text', path: 'description' },
      { name: 'permissions', type: 'jsonb', path: 'permissions' },
      { name: 'type', type: 'text', path: 'type' },
      ...timestamps,
    ],
    events: { upsert: ['role.created', 'role.updated'], delete: ['role.deleted'] },
    // Unpaginated: returns {object: 'list', data: []} with no list_metadata.
    list: { path: '/authorization/roles' },
  },
  {
    name: 'organization_roles',
    description: 'Organization-scoped custom roles',
    mode: 'mirror',
    group: 'authorization',
    key: ['organization_id', 'slug'],
    columns: [
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'slug', type: 'text', path: 'slug' },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'description', type: 'text', path: 'description' },
      { name: 'permissions', type: 'jsonb', path: 'permissions' },
      ...timestamps,
    ],
    events: {
      upsert: ['organization_role.created', 'organization_role.updated'],
      delete: ['organization_role.deleted'],
    },
    list: {
      path: '/authorization/organizations/{organization_id}/roles',
      parent: { stream: 'organizations', param: 'organization_id', from: 'path' },
    },
  },
  {
    name: 'permissions',
    description: 'Permissions',
    mode: 'mirror',
    group: 'authorization',
    key: ['slug'],
    prefixes: ['perm_'],
    columns: [
      { name: 'slug', type: 'text', path: 'slug' },
      { name: 'id', type: 'text', path: 'id' },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'description', type: 'text', path: 'description' },
      { name: 'system', type: 'boolean', path: 'system' },
      ...timestamps,
    ],
    events: {
      upsert: ['permission.created', 'permission.updated'],
      delete: ['permission.deleted'],
    },
    list: { path: '/authorization/permissions' },
  },
  {
    name: 'feature_flags',
    description: 'Feature flags. Note: flags are project-scoped, shared across environments',
    mode: 'mirror',
    group: 'authorization',
    key: ['slug'],
    prefixes: ['flag_'],
    columns: [
      { name: 'slug', type: 'text', path: 'slug' },
      { name: 'id', type: 'text', path: 'id' },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'description', type: 'text', path: 'description' },
      { name: 'enabled', type: 'boolean', path: 'enabled' },
      { name: 'default_value', type: 'jsonb', path: 'default_value' },
      { name: 'tags', type: 'jsonb', path: 'tags' },
      ...timestamps,
    ],
    events: {
      upsert: ['flag.created', 'flag.updated', 'flag.rule_updated'],
      delete: ['flag.deleted'],
    },
    list: { path: '/feature-flags' },
  },

  // ------------------------------------------------------------ poll-only
  // No events exist for these. They are snapshot-polled and are only as fresh
  // as the last `reconcile` run, so they are opt-in rather than default.
  {
    name: 'fga_resources',
    description: 'Fine-grained authorization resources (poll-only: no events exist)',
    mode: 'poll',
    group: 'authorization',
    key: ['id'],
    prefixes: ['authz_resource_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'external_id', type: 'text', path: 'external_id', index: true },
      { name: 'resource_type_slug', type: 'text', path: 'resource_type_slug', index: true },
      { name: 'organization_id', type: 'text', path: 'organization_id', index: true },
      { name: 'name', type: 'text', path: 'name' },
      { name: 'parent_resource_id', type: 'text', path: 'parent_resource_id' },
      ...timestamps,
    ],
    list: { path: '/authorization/resources' },
  },
  {
    name: 'webhook_endpoints',
    description: 'Configured webhook endpoints (poll-only: no events exist)',
    mode: 'poll',
    group: 'platform',
    key: ['id'],
    prefixes: ['we_'],
    columns: [
      { name: 'id', type: 'text', path: 'id' },
      { name: 'endpoint_url', type: 'text', path: 'endpoint_url' },
      { name: 'status', type: 'text', path: 'status' },
      { name: 'events', type: 'jsonb', path: 'events' },
      ...timestamps,
    ],
    list: { path: '/webhook_endpoints' },
    // The webhook endpoint object exposes its signing `secret`; mirroring it
    // verbatim would let any mirror reader forge signed webhook deliveries.
    redact: ['secret'],
  },
] satisfies CatalogStreamDef[]

/**
 * The columns of `stream` that name a user: those declared as referencing
 * `users`, and the `users` stream's own key. A membership's or sign-in's
 * `user_id`, an invitation's inviter and the user who accepted it. `redact`
 * erases a row that names the subject in any of them.
 */
export function userReferenceColumns(stream: StreamDef): ColumnDef[] {
  return stream.columns.filter(
    (c) => c.references === 'users' || (stream.name === 'users' && stream.key.includes(c.name)),
  )
}

/**
 * The top-level payload fields that name a user, across the catalog. An
 * event's `data` carries the object, so a field that references `users` on
 * any stream does there too; `redact` scrubs archived and quarantined
 * events by them.
 */
export function userReferenceFields(): string[] {
  const out = new Set<string>()
  for (const stream of STREAMS) {
    for (const col of stream.columns) {
      if (col.references === 'users' && typeof col.path === 'string') out.add(col.path)
    }
  }
  return [...out]
}

export const streamByName = (name: string): StreamDef | undefined => STREAMS.find((s) => s.name === name)

/**
 * Resolve an object ID to its stream by prefix, longest first.
 * `conn_app_01...` must not match the `conn_` stream.
 */
export const streamForId = (id: string): StreamDef | undefined => {
  let best: StreamDef | undefined
  let bestLen = -1
  for (const s of STREAMS) {
    for (const p of s.prefixes ?? []) {
      if (id.startsWith(p) && p.length > bestLen) {
        best = s
        bestLen = p.length
      }
    }
  }
  return best
}

/**
 * Streams synced by default: every stream that has events. Poll-only streams
 * have no events, cost a full re-list on every reconcile, and are only as
 * fresh as that last run, so they are opt-in.
 */
export const defaultStreams = (): string[] => STREAMS.filter((s) => s.mode !== 'poll').map((s) => s.name)

/**
 * The streams a configuration selects, resolved the way the engine resolves
 * them: the named streams or the defaults, always with `events` (every event
 * is archived there before it is routed, so that table has to exist whatever
 * else is selected), each with any `extraColumns` appended. Exported so a
 * caller that never builds an engine — the CLI printing the DDL — selects
 * exactly what the engine would.
 */
export function selectStreams(names?: string[], extraColumns: Record<string, ColumnDef[]> = {}): StreamDef[] {
  const selected = new Set([...(names ?? defaultStreams()), 'events'])
  for (const n of selected) {
    if (!streamByName(n)) throw new Error(`unknown stream: ${n}`)
  }
  const streams = STREAMS.filter((s) => selected.has(s.name)).map((s) =>
    extraColumns[s.name]?.length ? { ...s, columns: [...s.columns, ...extraColumns[s.name]!] } : s,
  )
  for (const name of Object.keys(extraColumns)) {
    if (!selected.has(name)) throw new Error(`extraColumns names a stream that is not selected: ${name}`)
  }
  if (streams.length === 0) throw new Error('no streams selected')
  return streams
}

/**
 * The event types to subscribe to on `GET /events`.
 *
 * The `events` parameter is **required** by the API: the live endpoint returns
 * `400 invalid_events_request` without it — verified against the API. The
 * published OpenAPI spec once marked it optional; it now marks it required,
 * consistent with the Node SDK, which types the field as required.
 *
 * The practical consequence is worth stating plainly: a consumer cannot ask
 * for "everything". It must enumerate what it wants, so a newly introduced
 * WorkOS event type does not begin arriving until this catalog is updated.
 * Deriving the list from the stream definitions keeps the two in step.
 *
 * Unknown *names* are rejected with a 400, so this list must only contain
 * event types that actually exist. The catalog is typed against the spec's
 * `EventSchema` union (`EventName` above), so a retired name fails typecheck.
 */
export const subscribedEventTypes = (streams: StreamDef[] = STREAMS): string[] => {
  const out = new Set<string>()
  for (const s of streams) {
    for (const e of s.events?.upsert ?? []) out.add(e)
    for (const e of s.events?.delete ?? []) out.add(e)
  }
  return [...out].sort()
}
