/**
 * Docker HEALTHCHECK for a long-running `listen`.
 *
 * The image has no curl or wget, so Node's global fetch does the probe. The
 * check runs in its own process and cannot ask the listener which port it is
 * serving, so it resolves the port the way `listen` does: a `--health-port
 * <n>` flag wins over WORKOS_SYNC_HEALTH_PORT. The flag is read from PID 1's
 * command line, which is the entrypoint itself under a plain `docker run`,
 * or an init (`--init`, tini) whose argv carries the same command. Anything
 * unreadable falls back to the variable, which the image sets to 8080.
 *
 * The loopback address is used rather than the bind host so the check works
 * whatever WORKOS_SYNC_HEALTH_HOST is set to, as long as it includes this
 * interface.
 */
import { readFileSync } from 'node:fs'

function effectivePort() {
  try {
    // /proc/<pid>/cmdline is argv joined by NUL bytes. The flag takes the
    // next token as its value unless that token is another flag, which is
    // the rule the CLI's own parser applies.
    const argv = readFileSync('/proc/1/cmdline', 'utf8').split('\0')
    const i = argv.indexOf('--health-port')
    const value = argv[i + 1]
    if (i !== -1 && value && !value.startsWith('--')) return value
  } catch {
    // Not Linux, or /proc is hidden: the variable is all there is.
  }
  return process.env.WORKOS_SYNC_HEALTH_PORT
}

fetch(`http://127.0.0.1:${effectivePort()}/healthz`).then(
  (res) => process.exit(res.ok ? 0 : 1),
  () => process.exit(1),
)
