# syntax=docker/dockerfile:1

# One Dockerfile for every destination in this workspace. The build arg names
# the package under packages/ (sync-<destination>) whose CLI becomes the
# image's entrypoint:
#
#   docker build --build-arg DESTINATION=postgres -t sync-postgres .
#   docker run --rm -e DATABASE_URL -e WORKOS_API_KEY sync-postgres listen
#
# A destination package needs a `bin` pointing at dist/cli.js and a `files`
# list that ships dist/. Nothing here changes when one is added.
ARG DESTINATION=postgres
ARG NODE_VERSION=24

# CI and release callers mount the Socket Firewall npmrc as a BuildKit secret
# and set SFW_REQUIRED=true. Local builds omit both and use the public registry.
ARG SFW_REQUIRED=false

# ---- Manifests stage ----
# Only what pnpm needs to resolve the workspace, found rather than listed, so
# adding a workspace package needs no new COPY line. The install layer below
# is keyed on these files alone and survives source-only changes.
FROM node:${NODE_VERSION}-alpine AS manifests
WORKDIR /app
COPY . .
RUN find . \
      \( -name 'package.json' -o -name 'pnpm-workspace.yaml' -o -name 'pnpm-lock.yaml' \) \
      -not -path '*/node_modules/*' \
      -exec sh -c 'dst="/m/$1"; mkdir -p "$(dirname "$dst")"; cp "$1" "$dst"' _ {} \;

# ---- Build stage ----
FROM node:${NODE_VERSION}-alpine AS build
ARG DESTINATION
ARG SFW_REQUIRED
WORKDIR /app

COPY --from=manifests /m/ ./

# pnpm comes from the `packageManager` pin through npm rather than corepack:
# corepack downloads with its own resolver that ignores the mounted npmrc, so
# it could not go through Socket Firewall. The filter installs the destination
# and the workspace packages it depends on; the examples package is skipped.
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    set -eu; \
    if [ ! -f "packages/sync-${DESTINATION}/package.json" ]; then \
      echo "DESTINATION=${DESTINATION} names no package: packages/sync-${DESTINATION}/package.json does not exist." >&2; \
      exit 1; \
    fi; \
    if [ "$SFW_REQUIRED" = "true" ] && [ ! -s /root/.npmrc ]; then \
      echo "Socket Firewall npmrc secret is required for Docker dependency installs." >&2; \
      exit 1; \
    fi; \
    npm install --global "$(node -p 'require("./package.json").packageManager')" \
      --ignore-scripts --no-audit --no-fund; \
    pnpm install --frozen-lockfile --filter "@workos/sync-${DESTINATION}..."

COPY . .

# Build the destination and the workspace packages it depends on, then
# `pnpm deploy` copies the package to /deploy with only its production
# dependencies, workspace ones included as real directories rather than
# symlinks into the monorepo.
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    pnpm --filter "@workos/sync-${DESTINATION}..." run build \
 && pnpm --filter "@workos/sync-${DESTINATION}" deploy --legacy --prod /deploy

# ---- Runtime stage ----
# Only the deployed package, its production dependencies, and Node.
FROM node:${NODE_VERSION}-alpine
ARG DESTINATION
LABEL org.opencontainers.image.source="https://github.com/workos/sync-engine"
LABEL org.opencontainers.image.title="workos-sync-${DESTINATION}"
LABEL org.opencontainers.image.licenses="MIT"
WORKDIR /app
RUN addgroup --system --gid 1001 nodejs && adduser --system --uid 1001 --ingroup nodejs workos
COPY --from=build /deploy/package.json ./
COPY --from=build /deploy/node_modules ./node_modules
COPY --from=build /deploy/dist ./dist
COPY docker/healthcheck.mjs ./healthcheck.mjs
USER workos
ENV NODE_ENV=production
# `listen` serves GET /healthz and /metrics on this port, from in-memory state,
# so an orchestrator can tell a stalled tail from a live one without a
# database connection. Only `listen` opens the port; one-shot commands (init,
# backfill, tick, ...) ignore it and exit long before the check below would
# run, so the HEALTHCHECK is only meaningful for a long-running `listen`.
ENV WORKOS_SYNC_HEALTH_PORT=8080
# The OpenTelemetry SDK is an optional dependency that `pnpm deploy` keeps, so
# `-e WORKOS_SYNC_OTEL=1` with the usual OTEL_* variables exports metrics from
# any command, one-shot ones included; see docs/operations.md.
EXPOSE 8080
# The check resolves the port the way `listen` does, so `listen --health-port
# <n>` keeps the container healthy just as `-e WORKOS_SYNC_HEALTH_PORT=<n>`
# does. healthcheck.mjs explains how it finds the flag.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["node", "healthcheck.mjs"]
# --use-env-proxy makes fetch honor HTTP_PROXY, HTTPS_PROXY and NO_PROXY, which
# is how the sync reaches the WorkOS API from inside a locked-down network.
ENTRYPOINT ["node", "--use-env-proxy", "dist/cli.js"]
CMD ["help"]
