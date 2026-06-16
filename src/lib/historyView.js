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
 * The decoded params/result/error payload to display under a timeline row, with a
 * short label, or `null` for events that carry none (timers, patches). `value` is
 * the already-decoded JSON the host sent (object, scalar, or null).
 * @param {EventDto & Record<string, any>} ev
 * @returns {{ label: string, value: any } | null}
 */
export function eventPayload(ev) {
  switch (ev.kind) {
    case 'WorkflowStarted':
    case 'ActivityScheduled':
    case 'ChildScheduled':
      return { label: 'params', value: ev.input };
    case 'ActivityCompleted':
      return { label: 'result', value: ev.output };
    case 'ChildCompleted':
      return { label: 'result', value: ev.result };
    case 'ActivityFailed':
      return { label: 'error', value: ev.error };
    case 'SignalReceived':
      return { label: 'payload', value: ev.payload };
    default:
      return null;
  }
}

/**
 * Pretty-print a decoded JSON value for display; '' for `undefined`.
 * @param {any} value
 * @returns {string}
 */
export function formatJson(value) {
  if (value === undefined) return '';
  return JSON.stringify(value, null, 2);
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
