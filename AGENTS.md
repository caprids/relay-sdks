# Agents

Repo-level instructions for coding agents. Human-facing docs start at `README.md`; the contract
every SDK implements lives in `spec/`.

## Agent skills

### Issue tracker

Issues live in GitHub Issues for `muxinc/relay-sdks`, driven by the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles map to labels of the same name (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` at the repo root plus `docs/adr/`, with `spec/vocabulary.md` as the existing glossary of public names. See `docs/agents/domain.md`.
