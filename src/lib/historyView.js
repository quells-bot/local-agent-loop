/**
 * @typedef {Object} EventDto
 * @property {number} event_id
 * @property {number} ts
 * @property {string} kind
 * @property {string|null} [child_run_id]
 * @property {number} [seq]
 * @property {string} [activity_type]
 * @property {string} [workflow_type]
 * @property {string} [name]
 * @property {number} [duration_ms]
 * @property {string} [change_id]
 */

/**
 * Short human label for one timeline row.
 * @param {EventDto} ev
 * @returns {string}
 */
export function eventLabel(ev) {
  switch (ev.kind) {
    case 'WorkflowStarted':
      return 'Workflow started';
    case 'ActivityScheduled':
      return `Activity scheduled: ${ev.activity_type} (#${ev.seq})`;
    case 'ActivityCompleted':
      return `Activity completed (#${ev.seq})`;
    case 'ActivityFailed':
      return `Activity failed (#${ev.seq})`;
    case 'TimerStarted':
      return `Timer started: ${ev.duration_ms}ms (#${ev.seq})`;
    case 'TimerFired':
      return `Timer fired (#${ev.seq})`;
    case 'SignalReceived':
      return `Signal received: ${ev.name}`;
    case 'ChildScheduled':
      return `Child scheduled: ${ev.workflow_type} (#${ev.seq})`;
    case 'ChildCompleted':
      return `Child completed (#${ev.seq})`;
    case 'Patched':
      return `Patched: ${ev.change_id}`;
    default:
      return ev.kind;
  }
}

/**
 * CSS modifier class for a run status.
 * @param {string} status
 * @returns {string}
 */
export function statusClass(status) {
  switch (status) {
    case 'completed':
      return 'status-completed';
    case 'failed':
      return 'status-failed';
    case 'running':
      return 'status-running';
    default:
      return 'status-unknown';
  }
}

/**
 * Epoch-ms → local datetime string; '' for a falsy timestamp.
 * @param {number} ms
 * @returns {string}
 */
export function formatTime(ms) {
  if (!ms) return '';
  return new Date(ms).toLocaleString();
}

/**
 * @typedef {Object} Crumb
 * @property {string} run_id
 * @property {string} label
 */

/**
 * Append a crumb to the breadcrumb trail.
 * @param {Crumb[]} stack
 * @param {Crumb} crumb
 * @returns {Crumb[]}
 */
export function pushCrumb(stack, crumb) {
  return [...stack, crumb];
}

/**
 * Truncate the trail back to (and including) the crumb at `index`.
 * @param {Crumb[]} stack
 * @param {number} index
 * @returns {Crumb[]}
 */
export function popTo(stack, index) {
  return stack.slice(0, index + 1);
}
