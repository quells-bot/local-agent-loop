/**
 * @typedef {Object} ServiceMessage  // mirrors the host's ChatMessageDto
 * @property {string} message_id
 * @property {'user'|'assistant'} role
 * @property {string} content
 * @property {'complete'|'error'} status
 * @property {number} seq
 */

/**
 * Are we still waiting for a reply to the latest user message? True when the most
 * recent user message has no matching "{message_id}-reply" row yet. Drives input
 * locking. Pure reducer — the frontend's unit-test seam.
 *
 * @param {ServiceMessage[]} messages  // ordered by seq
 * @returns {boolean}
 */
export function awaitingReply(messages) {
  const lastUser = [...messages].reverse().find((m) => m.role === 'user');
  if (!lastUser) return false;
  const replyId = `${lastUser.message_id}-reply`;
  return !messages.some((m) => m.message_id === replyId);
}

/**
 * Merge a just-sent (optimistic) user message into the polled service list. If the
 * service already has a row with the same message_id, the optimistic copy is
 * dropped (reconciled by id); otherwise it is appended. Pure reducer.
 *
 * @param {ServiceMessage[]} serviceMessages
 * @param {ServiceMessage|null} optimistic
 * @returns {ServiceMessage[]}
 */
export function mergeOptimistic(serviceMessages, optimistic) {
  if (!optimistic) return serviceMessages;
  if (serviceMessages.some((m) => m.message_id === optimistic.message_id)) {
    return serviceMessages;
  }
  return [...serviceMessages, optimistic];
}
