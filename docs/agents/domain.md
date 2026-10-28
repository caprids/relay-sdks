# Domain docs

How skills and agents use this repo's domain documentation when they explore the codebase.

## This repo

Single-context. `ts/` and `python/` are separate package workspaces, but they implement one
domain from one source of truth, so there is one `CONTEXT.md` at the root and one `docs/adr/`.

Two spec files already carry domain vocabulary and decisions. Read them alongside `CONTEXT.md`
and do not restate them:

- **`spec/vocabulary.md`** is the authoritative glossary of public SDK names across languages,
  with the rationale for each. `CONTEXT.md` links to it and adds only concepts that are not
  public API (internal terms, relay-side behavior, test vocabulary).
- **`spec/behavior.md`** is the behavior contract every SDK must exhibit. A decision that changes
  observable behavior belongs there, and an ADR records why it was made.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/adr/`**: the ADRs that touch the area you are about to work in.

The `/domain-modeling` skill adds glossary terms and ADRs when a term or decision gets resolved.

## File structure

```
/
├── AGENTS.md
├── CONTEXT.md                  ← glossary, links spec/vocabulary.md for public names
├── docs/
│   ├── adr/                    ← 0001-*.md onward, one decision each
│   └── agents/                 ← this file and its siblings
├── spec/                       ← protocol pins, behavior contract, vocabulary, vectors
├── ts/                         ← TypeScript SDK (pnpm workspace)
├── python/                     ← Python SDK (uv workspace)
└── tools/                      ← hardware measurement rigs
```

This repo does not use `CONTEXT-MAP.md` or per-context `docs/adr/` directories.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need is not in the glossary yet, either you are inventing language the project does not use (reconsider) or there is a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR 0001 (one relay subscription per track), but worth reopening because…_
