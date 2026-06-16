import { describe, it, expect } from 'vitest';
import { applyCompletion } from './applyCompletion.js';

/** @returns {import('./applyCompletion.js').Message[]} */
const pending = () => [{ id: 'wf-1', text: '1 2 3', status: 'pending' }];

describe('applyCompletion', () => {
  it('moves a pending message to done with the total', () => {
    const next = applyCompletion(pending(), {
      workflow_id: 'wf-1',
      run_id: 'r1',
      status: 'completed',
      result: { total: 6 }
    });
    expect(next).toEqual([{ id: 'wf-1', text: '1 2 3', status: 'done', output: 6 }]);
  });

  it('moves a pending message to error with the message', () => {
    const next = applyCompletion(pending(), {
      workflow_id: 'wf-1',
      run_id: 'r1',
      status: 'failed',
      result: { message: "could not parse 'two' as an integer" }
    });
    expect(next).toEqual([
      { id: 'wf-1', text: '1 2 3', status: 'error', error: "could not parse 'two' as an integer" }
    ]);
  });

  it('leaves non-matching messages untouched', () => {
    const before = pending();
    const next = applyCompletion(before, {
      workflow_id: 'other',
      run_id: 'r9',
      status: 'completed',
      result: { total: 42 }
    });
    expect(next).toEqual(before);
  });

  it('tolerates a null result on failure', () => {
    const next = applyCompletion(pending(), {
      workflow_id: 'wf-1',
      run_id: 'r1',
      status: 'failed',
      result: null
    });
    expect(next[0].status).toBe('error');
    expect(typeof next[0].error).toBe('string');
  });
});
