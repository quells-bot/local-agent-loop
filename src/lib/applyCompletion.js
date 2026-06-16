/**
 * @typedef {Object} Message
 * @property {string} id        // == workflow_id, the correlation key
 * @property {string} text      // the submitted input
 * @property {'pending'|'done'|'error'} status
 * @property {number} [output]  // set when status === 'done' (from result.total)
 * @property {string} [error]   // set when status === 'error' (from result.message)
 */

/**
 * @typedef {Object} CompletionPayload
 * @property {string} workflow_id
 * @property {string} run_id
 * @property {'completed'|'failed'} status
 * @property {{ total?: number, message?: string } | null} result
 */

/**
 * Pure reducer: given the current messages and a `run_completed` event payload,
 * return the next messages array with the matching message resolved. The
 * frontend's unit-test seam (spec §6).
 *
 * @param {Message[]} messages
 * @param {CompletionPayload} payload
 * @returns {Message[]}
 */
export function applyCompletion(messages, payload) {
  return messages.map((m) => {
    if (m.id !== payload.workflow_id) return m;
    if (payload.status === 'completed') {
      return { id: m.id, text: m.text, status: 'done', output: payload.result?.total };
    }
    const error = payload.result?.message ?? 'workflow failed';
    return { id: m.id, text: m.text, status: 'error', error };
  });
}
