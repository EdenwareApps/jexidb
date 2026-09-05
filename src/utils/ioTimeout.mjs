/**
 * Shared helpers to normalize and detect I/O timeout / abort errors.
 *
 * Node surfaces read timeouts in several inconsistent shapes depending on the
 * path used (`error.name`, `error.code` or `error.message`). These helpers
 * centralize creation and detection so every retry branch across Database.mjs
 * and FileHandler.mjs agrees, and timeouts are always delivered as normal
 * promise rejections - never as an `uncaughtException`.
 */

/**
 * Create a normalized I/O timeout error that every retry branch recognizes.
 * @param {number} timeoutMs - Timeout that elapsed, in milliseconds.
 * @returns {Error}
 */
export function createIoTimeoutError(timeoutMs) {
  const err = new Error(`I/O timeout after ${timeoutMs}ms`)
  err.name = 'AbortError'
  err.code = 'ETIMEDOUT'
  return err
}

/**
 * Detect whether an error represents an I/O timeout or abort, regardless of the
 * shape it was produced in.
 * @param {*} error - Error to inspect (may be null/undefined).
 * @returns {boolean}
 */
export function isIoTimeoutError(error) {
  if (!error) return false
  return (
    error.name === 'AbortError' ||
    error.code === 'ETIMEDOUT' ||
    error.message === 'AbortError'
  )
}
