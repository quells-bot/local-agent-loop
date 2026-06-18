# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Tauri desktop app built on a durable, Temporal-like workflow engine (Rust + SQLite) with a SvelteKit chat frontend. Engine crates live in `crates/` (`activity`, `workflow`, `engine`, `persist`, `demo`); the Tauri host is `src-tauri/` (package `app`) and the frontend is `src/`.

The engine deliberately mirrors Temporal's Go SDK (activities, workflows, child workflows, signals) so designs translate directly. Design specs live in `docs/` and are referenced from code as `spec §N` — preserve that convention when touching engine code, and consult the spec before changing replay/durability semantics.

## Commands

- `cargo test --workspace` — run all Rust tests (engine crates + Tauri app).
- `npm run tauri dev` — build and run the desktop app.
- `npm run check` — type-check the frontend (svelte-check).
- `npm run test` — frontend unit tests (Vitest).

Note: bare `cargo build`/`cargo test` only touches the engine crates — `default-members` excludes the Tauri `app`. Use `--workspace` (or `-p app`) to include it.

## Engine gotchas

- **Deterministic concurrency only.** Inside workflow code use deterministic combinators (`futures::join!`, `futures::select_biased!`). Do NOT use `futures::select!` or a bare `FuturesUnordered` — ordering must be deterministic for replay. This is enforced at runtime via a replay-divergence check, not at compile time, so a violation surfaces as a divergence panic rather than a build error.
- Every activity/workflow takes a named params struct and returns a named results struct — never bare primitives or tuples.

## Frontend gotchas

- SPA only (SvelteKit static adapter, no SSR — Tauri can't serve SSR).
- The Vite dev server is port 1420 (strict) and intentionally ignores `target/` and `src-tauri/` to avoid inotify exhaustion. Don't widen the watch globs.

## Repo etiquette

- Conventional commit messages (`feat(...)`, `fix(...)`, `chore(...)`).
- Feature branches named `feat/<topic>`.
