import { describe, it, expect } from 'vitest';
import { eventLabel, statusClass, formatTime, pushCrumb, popTo } from './historyView.js';

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
