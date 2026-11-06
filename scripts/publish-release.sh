#!/usr/bin/env bash
#
# Publish the packages of a merged release-please PR by hand.
#
# npm trusted publishing (OIDC) rejects this repository's tokens: GitHub
# issues them with the immutable subject format (repos created after
# 2026-07-15), which npm's registry does not accept yet (npm/cli#9969). So
# the npm step of release.yml fails on every release. This script does what
# that step would have done, from the same release tags, and then reruns the
# failed Release Please run so the container images get built: the npm step
# sees each version already on npm and skips, and the GHCR job runs.
#
# Usage:
#   scripts/publish-release.sh [--pr <number>] [--dry-run] [--skip-tests] [--no-rerun]
#
#   --pr <number>  The merged release PR ("chore: release main"). Defaults to
#                  the most recently merged one.
#   --dry-run      Build and pack, then `npm publish --dry-run`. Reruns nothing.
#   --skip-tests   Do not run the test suites before packing.
#   --no-rerun     Publish only; leave the failed workflow run alone.
#
# Needs git, pnpm, npm, gh, node and curl, and an `npm login` against
# registry.npmjs.org. `npm publish` asks for a one-time password, so run this
# in a terminal. Versions already on npm are skipped, so rerunning after a
# partial failure is safe. Delete this script once npm/cli#9969 is fixed.

set -euo pipefail

REGISTRY="https://registry.npmjs.org/"
REPO="workos/sync-engine"
REPO_ROOT="$(git rev-parse --show-toplevel)"

PR=""
DRY_RUN=false
SKIP_TESTS=false
RERUN=true

usage() {
  sed -n '/^# Usage:/,/^#$/p' "$0" | sed -E 's/^# ?//'
}
log() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --pr) [ $# -ge 2 ] || die "--pr needs a number"; PR="$2"; shift 2 ;;
    --pr=*) PR="${1#--pr=}"; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --skip-tests) SKIP_TESTS=true; shift ;;
    --no-rerun) RERUN=false; shift ;;
    -h | --help) usage; exit 0 ;;
    [0-9]*) PR="$1"; shift ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

for tool in git pnpm npm gh node curl; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is required"
done

# --- Which release ------------------------------------------------------------

if [ -z "$PR" ]; then
  PR="$(gh pr list --repo "$REPO" --state merged --search 'chore: release main in:title' \
    --limit 1 --json number --jq '.[0].number // empty')"
  [ -n "$PR" ] || die "no merged release PR found; pass --pr <number>"
fi

IFS=$'\t' read -r STATE SHA TITLE < <(
  gh pr view "$PR" --repo "$REPO" --json state,mergeCommit,title \
    --jq '[.state, (.mergeCommit.oid // ""), .title] | join("\t")'
)
[ "$STATE" = "MERGED" ] || die "PR #$PR is $STATE, not merged"
[ -n "$SHA" ] || die "PR #$PR has no merge commit"
log "PR #$PR: $TITLE (merged as ${SHA:0:7})"

log "Fetching main and tags"
git -C "$REPO_ROOT" fetch --quiet origin \
  "+refs/heads/main:refs/remotes/origin/main" "+refs/tags/*:refs/tags/*"
git -C "$REPO_ROOT" merge-base --is-ancestor "$SHA" refs/remotes/origin/main ||
  die "${SHA:0:7} is not on origin/main"

# release-please tags each released component on the merge commit, and
# release.yml checks out that tag. No tags means its run has not finished.
TAGS="$(git -C "$REPO_ROOT" tag --points-at "$SHA" | grep -E '^[a-z0-9-]+-v[0-9]' || true)"
[ -n "$TAGS" ] ||
  die "no release tags point at ${SHA:0:7} yet; wait for the Release Please run: gh run list --repo $REPO --commit $SHA"
# The engine first: every destination pins it.
ORDERED=()
for TAG in $TAGS; do case "$TAG" in sync-engine-v*) ORDERED+=("$TAG") ;; esac; done
for TAG in $TAGS; do case "$TAG" in sync-engine-v*) ;; *) ORDERED+=("$TAG") ;; esac; done

# --- Source checkout ----------------------------------------------------------

WORK="$(mktemp -d "${TMPDIR:-/tmp}/sync-engine-release.XXXXXX")"
cleanup() {
  git -C "$REPO_ROOT" worktree remove --force "$WORK/src" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT
git -C "$REPO_ROOT" worktree add --quiet --detach "$WORK/src" "$SHA"
mkdir -p "$WORK/pack"

# --- Plan ---------------------------------------------------------------------

COMPONENTS=() VERSIONS=() NAMES=() NPM_TAGS=() STATUSES=()
PENDING=0
for TAG in "${ORDERED[@]}"; do
  COMPONENT="${TAG%-v*}"
  VERSION="${TAG##*-v}"
  PKG="$WORK/src/packages/$COMPONENT/package.json"
  [ -f "$PKG" ] || die "tag $TAG names no package: packages/$COMPONENT/package.json does not exist"
  NAME="$(node -p "require('$PKG').name")"
  PKG_VERSION="$(node -p "require('$PKG').version")"
  [ "$PKG_VERSION" = "$VERSION" ] || die "$NAME is $PKG_VERSION in package.json but tag $TAG says $VERSION"
  case "$VERSION" in *-*) NPM_TAG=beta ;; *) NPM_TAG=latest ;; esac
  if npm view "$NAME@$VERSION" version --registry "$REGISTRY" >/dev/null 2>&1; then
    STATUS="on npm already"
  else
    STATUS="to publish"
    PENDING=$((PENDING + 1))
  fi
  COMPONENTS+=("$COMPONENT") VERSIONS+=("$VERSION") NAMES+=("$NAME") NPM_TAGS+=("$NPM_TAG") STATUSES+=("$STATUS")
done

log "Release plan"
for i in "${!NAMES[@]}"; do
  printf '  %-28s %-12s %-7s %s\n' "${NAMES[$i]}" "${VERSIONS[$i]}" "${NPM_TAGS[$i]}" "${STATUSES[$i]}"
done

if [ "$PENDING" -eq 0 ] && ! $DRY_RUN; then
  log "Every version is on npm already; nothing to publish"
else
  if ! $DRY_RUN; then
    npm whoami --registry "$REGISTRY" >/dev/null 2>&1 ||
      die "not logged in to $REGISTRY; run: npm login --registry $REGISTRY"
    [ -t 0 ] || echo "warning: stdin is not a terminal; npm publish may fail to ask for a one-time password" >&2
  fi

  # --- Build ------------------------------------------------------------------

  log "Installing and building at ${SHA:0:7}"
  (cd "$WORK/src" && pnpm install --frozen-lockfile && pnpm build)
  if $SKIP_TESTS; then
    log "Skipping tests"
  else
    log "Testing"
    (cd "$WORK/src" && pnpm test)
  fi

  # --- Pack and publish -------------------------------------------------------

  PUBLISHED=()
  for i in "${!NAMES[@]}"; do
    NAME="${NAMES[$i]}" VERSION="${VERSIONS[$i]}" COMPONENT="${COMPONENTS[$i]}" NPM_TAG="${NPM_TAGS[$i]}"
    if [ "${STATUSES[$i]}" = "on npm already" ] && ! $DRY_RUN; then
      echo "skip $NAME@$VERSION: on npm already"
      continue
    fi
    log "Packing $NAME@$VERSION"
    # `pnpm pack` rewrites `workspace:*` to the dependency's version in this
    # checkout, which a plain `npm publish` of the directory would not.
    TARBALL="$(cd "$WORK/src" && pnpm --filter "./packages/$COMPONENT" pack --json --pack-destination "$WORK/pack" |
      node -p "JSON.parse(require('fs').readFileSync(0, 'utf8')).filename")"
    TGZ_VERSION="$(tar xzOf "$TARBALL" package/package.json | node -p "JSON.parse(require('fs').readFileSync(0, 'utf8')).version")"
    [ "$TGZ_VERSION" = "$VERSION" ] || die "$TARBALL says $TGZ_VERSION, expected $VERSION"
    if $DRY_RUN; then
      if [ "${STATUSES[$i]}" = "on npm already" ]; then
        # `npm publish --dry-run` still refuses a version that is on npm.
        log "Dry run: packed $(basename "$TARBALL"); would skip, on npm already"
      else
        log "Dry run: npm publish $(basename "$TARBALL") --tag $NPM_TAG"
        npm publish "$TARBALL" --dry-run --access public --tag "$NPM_TAG" --registry "$REGISTRY"
      fi
    else
      log "Publishing $NAME@$VERSION with tag $NPM_TAG"
      npm publish "$TARBALL" --access public --tag "$NPM_TAG" --registry "$REGISTRY"
      PUBLISHED+=("$NAME@$VERSION")
    fi
  done

  # The registry's read side lags a publish by up to a couple of minutes, and
  # the rerun below checks it before deciding to skip.
  if [ "${#PUBLISHED[@]}" -gt 0 ]; then
    log "Waiting for the registry to show the new versions"
    for SPEC in "${PUBLISHED[@]}"; do
      n=0
      until npm view "$SPEC" version --registry "$REGISTRY" >/dev/null 2>&1; do
        n=$((n + 1))
        [ "$n" -le 40 ] || die "$SPEC is not visible after 10 minutes; check npmjs.com before rerunning the workflow"
        sleep 15
      done
      echo "$SPEC is live"
    done
  fi
fi

# --- Container images ---------------------------------------------------------

if $DRY_RUN; then
  log "Dry run: not rerunning the Release Please run"
  exit 0
fi
if ! $RERUN; then
  log "Skipping the workflow rerun (--no-rerun)"
  exit 0
fi

log "Rerunning the failed Release Please run for ${SHA:0:7} to build the container images"
RUN_LINE="$(gh run list --repo "$REPO" --workflow release-please.yml --commit "$SHA" --limit 1 \
  --json databaseId,status,conclusion --jq '.[0] | select(. != null) | [(.databaseId | tostring), .status, (.conclusion // "none")] | join(" ")')"
if [ -z "$RUN_LINE" ]; then
  echo "no Release Please run found for ${SHA:0:7}; rerun it by hand when it exists"
  exit 0
fi
read -r RUN_ID RUN_STATUS RUN_CONCLUSION <<<"$RUN_LINE"
RUN_URL="https://github.com/$REPO/actions/runs/$RUN_ID"
if [ "$RUN_STATUS" != "completed" ]; then
  echo "run $RUN_ID is $RUN_STATUS; waiting for it to finish"
  gh run watch "$RUN_ID" --repo "$REPO" >/dev/null 2>&1 || true
  RUN_CONCLUSION="$(gh run view "$RUN_ID" --repo "$REPO" --json conclusion --jq '.conclusion // "none"')"
fi
case "$RUN_CONCLUSION" in
  success)
    echo "run $RUN_ID already succeeded; nothing to rerun ($RUN_URL)"
    ;;
  *)
    gh run rerun "$RUN_ID" --repo "$REPO" --failed
    echo "rerun started: $RUN_URL"
    gh run watch "$RUN_ID" --repo "$REPO" --exit-status >/dev/null 2>&1 ||
      die "the rerun failed; see $RUN_URL"
    echo "container images published"
    ;;
esac

log "Done"
