import { describe, it, expect } from 'vitest';
import {
  eventLabel,
  eventPayload,
  formatJson,
  statusClass,
  formatTime,
  pushCrumb,
  popTo
} from './historyView.js';

describe('eventLabel', () => {
  it('labels an activity-scheduled row with type and seq', () => {
    expect(eventLabel({ kind: 'ActivityScheduled', activity_type: 'Parse', seq: 0 })).toBe(
      'Activity scheduled: Parse (#0)'
    );
  });

  it('labels a child-scheduled row', () => {
    expect(eventLabel({ kind: 'ChildScheduled', workflow_type: 'SumChild', seq: 1 })).toBe(
      'Child scheduled: SumChild (#1)'
    );
  });

  it('falls back to the raw kind for unknown events', () => {
    expect(eventLabel({ kind: 'Mystery' })).toBe('Mystery');
  });

  it('labels a timer-started row with duration and seq', () => {
    expect(eventLabel({ kind: 'TimerStarted', duration_ms: 500, seq: 2 })).toBe(
      'Timer started: 500ms (#2)'
    );
  });

  it('labels a patched row with change_id', () => {
    expect(eventLabel({ kind: 'Patched', change_id: 'my-change' })).toBe('Patched: my-change');
  });
});

describe('eventPayload', () => {
  it('shows workflow/activity/child params from input', () => {
    expect(eventPayload({ kind: 'WorkflowStarted', input: { text: '1 2 3' } })).toEqual({
      label: 'params',
      value: { text: '1 2 3' }
    });
    expect(
      eventPayload({ kind: 'ActivityScheduled', input: { values: [1, 2] } })
    ).toEqual({ label: 'params', value: { values: [1, 2] } });
    expect(eventPayload({ kind: 'ChildScheduled', input: { values: [1] } })).toEqual({
      label: 'params',
      value: { values: [1] }
    });
  });

  it('shows activity result from output', () => {
    expect(eventPayload({ kind: 'ActivityCompleted', output: { total: 6 } })).toEqual({
      label: 'result',
      value: { total: 6 }
    });
  });

  it('shows child result and activity error', () => {
    expect(
      eventPayload({ kind: 'ChildCompleted', result: { status: 'completed', output: { total: 6 } } })
    ).toEqual({ label: 'result', value: { status: 'completed', output: { total: 6 } } });
    expect(
      eventPayload({ kind: 'ActivityFailed', error: { message: 'boom', non_retryable: true } })
    ).toEqual({ label: 'error', value: { message: 'boom', non_retryable: true } });
  });

  it('returns null for events without a payload', () => {
    expect(eventPayload({ kind: 'TimerFired', seq: 0 })).toBeNull();
    expect(eventPayload({ kind: 'Patched', change_id: 'x' })).toBeNull();
  });
});

describe('formatJson', () => {
  it('pretty-prints a decoded value', () => {
    expect(formatJson({ total: 6 })).toBe('{\n  "total": 6\n}');
  });
  it('returns empty string for undefined', () => {
    expect(formatJson(undefined)).toBe('');
  });
  it('renders null as the literal null', () => {
    expect(formatJson(null)).toBe('null');
  });
});

describe('statusClass', () => {
  it('maps known statuses', () => {
    expect(statusClass('completed')).toBe('status-completed');
    expect(statusClass('failed')).toBe('status-failed');
    expect(statusClass('running')).toBe('status-running');
  });
  it('maps unknown to status-unknown', () => {
    expect(statusClass('weird')).toBe('status-unknown');
  });
});

describe('formatTime', () => {
  it('returns empty string for a falsy timestamp', () => {
    expect(formatTime(0)).toBe('');
  });
  it('returns a non-empty string for a real timestamp', () => {
    expect(formatTime(1_700_000_000_000).length).toBeGreaterThan(0);
  });
});

describe('breadcrumb stack', () => {
  it('pushCrumb appends', () => {
    expect(pushCrumb([{ run_id: 'a' }], { run_id: 'b' })).toEqual([
      { run_id: 'a' },
      { run_id: 'b' }
    ]);
  });
  it('popTo truncates back to and including the index', () => {
    const stack = [{ run_id: 'a' }, { run_id: 'b' }, { run_id: 'c' }];
    expect(popTo(stack, 0)).toEqual([{ run_id: 'a' }]);
    expect(popTo(stack, 1)).toEqual([{ run_id: 'a' }, { run_id: 'b' }]);
  });
});
