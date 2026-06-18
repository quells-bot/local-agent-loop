<script>
  import { onMount, onDestroy } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { getCurrentWindow } from '@tauri-apps/api/window';
  import { awaitingReply, mergeOptimistic } from '$lib/chat.js';

  /** @type {string} */
  let conversationId = '';
  /** @type {import('$lib/chat.js').ServiceMessage[]} */
  let serviceMessages = $state([]);
  /** @type {import('$lib/chat.js').ServiceMessage | null} */
  let optimistic = $state(null);
  let draft = $state('');

  /** @type {ReturnType<typeof setInterval> | undefined} */
  let pollTimer;
  /** @type {(() => void) | undefined} */
  let unlistenClose;

  const rendered = $derived(mergeOptimistic(serviceMessages, optimistic));
  const locked = $derived(awaitingReply(rendered));

  async function poll() {
    if (!conversationId) return;
    try {
      serviceMessages = await invoke('chat_history', { conversationId });
      const opt = optimistic;
      if (opt && serviceMessages.some((m) => m.message_id === opt.message_id)) {
        optimistic = null; // service caught up; drop the optimistic copy
      }
    } catch {
      // transient read error; the next tick retries
    }
  }

  onMount(async () => {
    conversationId = crypto.randomUUID();
    await invoke('open_chat', { conversationId });
    await poll();
    pollTimer = setInterval(poll, 500);
    unlistenClose = await getCurrentWindow().onCloseRequested(async () => {
      await invoke('close_chat', { conversationId });
      // default behavior proceeds with the close after the signal is durable
    });
  });

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
    if (unlistenClose) unlistenClose();
  });

  async function send() {
    const text = draft.trim();
    if (text === '' || locked) return;
    const messageId = crypto.randomUUID();
    optimistic = {
      message_id: messageId,
      role: 'user',
      content: text,
      status: 'complete',
      seq: Number.MAX_SAFE_INTEGER
    };
    try {
      await invoke('send_message', { conversationId, messageId, text });
      draft = '';
    } catch (e) {
      console.error('send_message failed', e);
    }
  }
</script>

<main>
  <h1>Chat</h1>

  <div class="transcript">
    {#each rendered as m (m.message_id)}
      <div class="bubble {m.role} {m.status === 'error' ? 'error' : ''}">{m.content}</div>
    {/each}
    {#if locked}
      <div class="bubble assistant pending">…</div>
    {/if}
  </div>

  <form onsubmit={(e) => { e.preventDefault(); send(); }}>
    <input placeholder="Type a message" bind:value={draft} disabled={locked} />
    <button type="submit" disabled={locked}>Send</button>
  </form>
</main>

<style>
  main { max-width: 40rem; margin: 0 auto; padding: 1rem; font-family: system-ui, sans-serif; }
  .transcript { display: flex; flex-direction: column; gap: 0.5rem; height: 60vh; overflow-y: auto; padding: 0.5rem; border: 1px solid #ddd; border-radius: 8px; }
  .bubble { padding: 0.4rem 0.7rem; border-radius: 12px; max-width: 75%; white-space: pre-wrap; }
  .user { align-self: flex-end; background: #2563eb; color: white; }
  .assistant { align-self: flex-start; background: #f1f1f1; }
  .assistant.error { background: #fee2e2; color: #991b1b; }
  .assistant.pending { opacity: 0.6; }
  form { display: flex; gap: 0.5rem; margin-top: 0.75rem; }
  input { flex: 1; padding: 0.5rem; }
  button { padding: 0.5rem 1rem; }
</style>
