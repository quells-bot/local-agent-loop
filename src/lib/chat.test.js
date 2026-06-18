import { describe, it, expect } from 'vitest';
import { awaitingReply, mergeOptimistic } from './chat.js';

const user = (id, content) => ({ message_id: id, role: 'user', content, status: 'complete', seq: 0 });
const assistant = (id, content, status = 'complete') => ({
  message_id: id, role: 'assistant', content, status, seq: 0
});

describe('awaitingReply', () => {
  it('is false for an empty transcript', () => {
    expect(awaitingReply([])).toBe(false);
  });

  it('is true when the latest user message has no reply yet', () => {
    expect(awaitingReply([user('m1', 'hi')])).toBe(true);
  });

  it('is false once the matching reply lands (complete)', () => {
    expect(awaitingReply([user('m1', 'hi'), assistant('m1-reply', 'yo')])).toBe(false);
  });

  it('is false once the matching reply lands as error', () => {
    expect(awaitingReply([user('m1', 'hi'), assistant('m1-reply', 'boom', 'error')])).toBe(false);
  });

  it('is true again after a second user message before its reply', () => {
    expect(
      awaitingReply([user('m1', 'hi'), assistant('m1-reply', 'yo'), user('m2', 'again')])
    ).toBe(true);
  });
});

describe('mergeOptimistic', () => {
  it('returns the service list unchanged when there is no optimistic message', () => {
    const svc = [user('m1', 'hi')];
    expect(mergeOptimistic(svc, null)).toBe(svc);
  });

  it('appends the optimistic message when the service lacks it', () => {
    const merged = mergeOptimistic([], user('m1', 'hi'));
    expect(merged.map((m) => m.message_id)).toEqual(['m1']);
  });

  it('drops the optimistic copy once the service has the same id', () => {
    const svc = [user('m1', 'hi')];
    const merged = mergeOptimistic(svc, user('m1', 'hi'));
    expect(merged).toEqual(svc);
  });
});
