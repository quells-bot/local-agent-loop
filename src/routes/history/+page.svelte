<script>
  import { onMount } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { eventLabel, statusClass, formatTime, pushCrumb, popTo } from '$lib/historyView.js';

  /** @type {import('$lib/applyCompletion.js').Message[] | any[]} */
  let runs = $state([]);
  /** @type {any} */
  let detail = $state(null);
  /** @type {{ run_id: string, label: string }[]} */
  let crumbs = $state([]);
  let listError = $state('');
  let detailError = $state('');

  async function loadRuns() {
    try {
      runs = await invoke('list_runs');
      listError = '';
    } catch (e) {
      listError = String(e);
    }
  }

  /** @param {string} runId @param {string} label @param {boolean} drill */
  async function openRun(runId, label, drill) {
    try {
      detail = await invoke('run_detail', { runId });
      detailError = '';
      crumbs = drill ? pushCrumb(crumbs, { run_id: runId, label }) : [{ run_id: runId, label }];
    } catch (e) {
      detailError = String(e);
    }
  }

  /** @param {number} i */
  async function gotoCrumb(i) {
    const c = crumbs[i];
    crumbs = popTo(crumbs, i);
    try {
      detail = await invoke('run_detail', { runId: c.run_id });
      detailError = '';
    } catch (e) {
      detailError = String(e);
    }
  }

  /** @param {any} run */
  const runLabel = (run) => `${run.workflow_type} · ${run.run_id.slice(0, 8)}`;

  onMount(() => {
    loadRuns();
    const onFocus = () => loadRuns();
    window.addEventListener('focus', onFocus);
    return () => window.removeEventListener('focus', onFocus);
  });
</script>

<main>
  <header>
    <h1>Workflow History</h1>
    <button onclick={loadRuns}>Refresh</button>
  </header>

  <div class="panes">
    <section class="list">
      {#if listError}
        <p class="error">{listError}</p>
      {/if}
      {#each runs as run (run.run_id)}
        <button class="run-row" onclick={() => openRun(run.run_id, runLabel(run), false)}>
          <span class="badge {statusClass(run.status)}">{run.status}</span>
          <span class="rtype">{run.workflow_type}</span>
          <span class="rid">{run.run_id.slice(0, 8)}</span>
          <span class="rtime">{formatTime(run.started_at)}</span>
        </button>
      {/each}
      {#if runs.length === 0 && !listError}
        <p class="empty">No runs yet.</p>
      {/if}
    </section>

    <section class="detail">
      {#if detailError}
        <p class="error">{detailError}</p>
      {/if}
      {#if detail}
        {#if crumbs.length > 1}
          <nav class="crumbs">
            {#each crumbs as c, i}
              <button class="crumb" onclick={() => gotoCrumb(i)}>{c.label}</button>
              {#if i < crumbs.length - 1}<span class="sep">›</span>{/if}
            {/each}
          </nav>
        {/if}

        <div class="detail-head">
          <span class="badge {statusClass(detail.summary.status)}">{detail.summary.status}</span>
          <strong>{detail.summary.workflow_type}</strong>
          <code>{detail.summary.run_id}</code>
          <span class="rtime">{formatTime(detail.summary.started_at)}</span>
        </div>

        <ol class="timeline">
          {#each detail.events as ev (ev.event_id)}
            <li>
              <span class="ev-time">{formatTime(ev.ts)}</span>
              {#if ev.child_run_id}
                <button
                  class="ev-link"
                  onclick={() => openRun(ev.child_run_id, ev.workflow_type ?? 'child', true)}
                >
                  {eventLabel(ev)} →
                </button>
              {:else}
                <span class="ev-label">{eventLabel(ev)}</span>
              {/if}
            </li>
          {/each}
        </ol>
      {:else if !detailError}
        <p class="empty">Select a run to view its timeline.</p>
      {/if}
    </section>
  </div>
</main>

<style>
  main { max-width: 60rem; margin: 0 auto; padding: 1rem; font-family: system-ui, sans-serif; }
  header { display: flex; justify-content: space-between; align-items: center; }
  .panes { display: grid; grid-template-columns: 18rem 1fr; gap: 1rem; height: 70vh; }
  .list, .detail { overflow-y: auto; border: 1px solid #ddd; border-radius: 8px; padding: 0.5rem; }
  .run-row { display: flex; gap: 0.5rem; align-items: center; width: 100%; text-align: left;
    background: none; border: 0; border-bottom: 1px solid #eee; padding: 0.5rem 0.25rem; cursor: pointer; }
  .run-row:hover { background: #f6f6f6; }
  .rtype { font-weight: 600; }
  .rid { color: #666; font-family: ui-monospace, monospace; }
  .rtime, .ev-time { color: #888; font-size: 0.8rem; margin-left: auto; }
  .badge { font-size: 0.7rem; padding: 0.1rem 0.4rem; border-radius: 999px; text-transform: uppercase; }
  .status-completed { background: #dcfce7; color: #166534; }
  .status-failed { background: #fee2e2; color: #991b1b; }
  .status-running { background: #e0e7ff; color: #3730a3; }
  .status-unknown { background: #f1f1f1; color: #555; }
  .crumbs { display: flex; gap: 0.4rem; align-items: center; margin-bottom: 0.5rem; }
  .crumb { background: none; border: 0; color: #2563eb; cursor: pointer; padding: 0; }
  .sep { color: #999; }
  .detail-head { display: flex; gap: 0.5rem; align-items: center; margin-bottom: 0.75rem; }
  .timeline { list-style: none; margin: 0; padding: 0; }
  .timeline li { display: flex; gap: 0.5rem; align-items: baseline; padding: 0.3rem 0; border-bottom: 1px solid #f0f0f0; }
  .ev-link { background: none; border: 0; color: #2563eb; cursor: pointer; padding: 0; text-align: left; }
  .error { color: #991b1b; }
  .empty { color: #888; }
</style>
