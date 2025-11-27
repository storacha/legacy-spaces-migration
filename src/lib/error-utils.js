/**
 * Error handling utilities
 */

/**
 * Safely extract error message from unknown error type
 * @param {unknown} error
 * @returns {string}
 */
export function getErrorMessage(error) {
  return error instanceof Error ? error.message : String(error)
}
