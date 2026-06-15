<script>
  import { onMount } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { listen } from '@tauri-apps/api/event';
  import { applyCompletion } from '$lib/applyCompletion.js';

  /** @type {import('$lib/applyCompletion.js').Message[]} */
  let messages = $state([]);
  let draft = $state('');

  onMount(() => {
    const unlisten = listen('run_completed', (event) => {
      messages = applyCompletion(messages, /** @type {any} */ (event.payload));
    });
    return () => {
      unlisten.then((off) => off());
    };
  });

  async function submit() {
    const text = draft.trim();
    if (text === '') return;
    const id = crypto.randomUUID();
    messages = [...messages, { id, text, status: 'pending' }];
    draft = '';
    try {
      await invoke('submit', { text, workflowId: id });
    } catch (e) {
      messages = messages.map((m) =>
        m.id === id ? { ...m, status: 'error', error: String(e) } : m
      );
    }
  }
</script>

<main>
  <h1>Workflow Chat</h1>

  <div class="transcript">
    {#each messages as m (m.id)}
      <div class="bubble user">{m.text}</div>
      {#if m.status === 'pending'}
        <div class="bubble reply pending">…</div>
      {:else if m.status === 'done'}
        <div class="bubble reply">{m.output}</div>
      {:else if m.status === 'error'}
        <div class="bubble reply error">{m.error}</div>
      {/if}
    {/each}
  </div>

  <form onsubmit={(e) => { e.preventDefault(); submit(); }}>
    <input placeholder="space-separated integers, e.g. 1 2 3" bind:value={draft} />
    <button type="submit">Send</button>
  </form>
</main>

<style>
  main { max-width: 40rem; margin: 0 auto; padding: 1rem; font-family: system-ui, sans-serif; }
  .transcript { display: flex; flex-direction: column; gap: 0.5rem; height: 60vh; overflow-y: auto; padding: 0.5rem; border: 1px solid #ddd; border-radius: 8px; }
  .bubble { padding: 0.4rem 0.7rem; border-radius: 12px; max-width: 75%; }
  .user { align-self: flex-end; background: #2563eb; color: white; }
  .reply { align-self: flex-start; background: #f1f1f1; }
  .reply.error { background: #fee2e2; color: #991b1b; }
  .reply.pending { opacity: 0.6; }
  form { display: flex; gap: 0.5rem; margin-top: 0.75rem; }
  input { flex: 1; padding: 0.5rem; }
  button { padding: 0.5rem 1rem; }
</style>
