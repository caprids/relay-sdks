# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## This repo

Single-context. `ts/` and `python/` are separate package workspaces, but they implement one
domain from one source of truth, so there is one `CONTEXT.md` at the root and one `docs/adr/`.

Two spec files already carry domain vocabulary and decisions. Read them alongside `CONTEXT.md`
and do not restate them:

- **`spec/vocabulary.md`** is the authoritative glossary of public SDK names across languages,
  with the rationale for each. `CONTEXT.md` links to it and adds only concepts that are not
  public API (internal terms, relay-side behavior, test vocabulary).
- **`spec/behavior.md`** is the behavior contract every SDK must exhibit. A decision that changes
  observable behavior belongs there; an ADR records why it was made.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root, or
- **`CONTEXT-MAP.md`** at the repo root if it exists: it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- **`docs/adr/`**: read ADRs that touch the area you're about to work in. In multi-context repos, also check `src/<context>/docs/adr/` for context-scoped decisions.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

```
/
├── AGENTS.md
├── CONTEXT.md                  ← glossary; links spec/vocabulary.md for public names
├── docs/
│   ├── adr/
│   │   └── 0001-<decision>.md
│   └── agents/                 ← this file and its siblings
├── spec/                       ← protocol pins, behavior contract, vocabulary, vectors
├── ts/                         ← TypeScript SDK (pnpm workspace)
└── python/                     ← Python SDK (uv workspace)
```

This repo does not use `CONTEXT-MAP.md` or per-context `docs/adr/` directories.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders), but worth reopening because…_
