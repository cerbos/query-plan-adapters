# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues on `cerbos/query-plan-adapters`. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v` — `gh` does this automatically when run inside a clone.

## Scope labels

Every adapter has a label matching its directory name, plus one cross-cutting `conformance` label.
**When an issue applies to specific adapters, tag each one.** These are additive and orthogonal to
the triage labels in `docs/agents/triage-labels.md` — an issue normally carries one triage label
plus one or more scope labels.

| Label | Adapter |
| ------------------- | ------------------------- |
| `prisma` | Prisma (TypeScript) |
| `mongoose` | Mongoose (TypeScript) |
| `drizzle` | Drizzle ORM (TypeScript) |
| `convex` | Convex (TypeScript) |
| `langchain-chromadb` | LangChain/ChromaDB (TypeScript) |
| `sqlalchemy` | SQLAlchemy (Python) |
| `activerecord` | ActiveRecord (Ruby) |
| `ent` | Ent (Go) |
| `pgx` | pgx / PostgreSQL (Go) |
| `elasticsearch-java` | Elasticsearch (Java) |
| `spring-data` | Spring Data JPA (Java) |

And one cross-cutting label:

| Label | Scope |
| ------------- | --------------------------------------------------------------- |
| `conformance` | The shared adversarial corpus in `conformance/` — affects every adapter |

Rules:

- **Tag every adapter the issue actually touches**, not just the one the reporter happened to hit.
  A translation bug reported against one adapter has historically been the same bug in several —
  see "Changing how a condition is translated" in `CLAUDE.md`.
- **Use `conformance` instead of tagging every adapter** when the issue is about the corpus itself: a new
  hostile shape, a classification in `actions.json`, the wire fixtures, the degeneracy guard, or a
  `CERBOS_VERSION` bump. It already means "affects every adapter", so don't also apply a label per adapter
  — that's noise.
- **Combine the two when a corpus change has a known adapter-specific consequence.** `conformance` +
  `prisma` reads as "a corpus change whose open work is in the Prisma adapter". Adding a shape
  requires classifying it for every adapter, so only tag the adapters with outstanding work, and say in
  the body that the rest are already classified.
- **A change to `demo/` is also repo-wide** — the demo domain feeds every example application — but
  it isn't corpus work, so it takes no scope label unless a specific adapter is implicated.
- **No scope label** means the issue is repo-level: tooling, CI, docs, dependencies.

Adding an adapter to the repo means adding its label here and on GitHub in the same change.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either — resolve with `gh pr view 42` and fall back to `gh issue view 42`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue (`gh api` on the sub-issues endpoint). Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: GitHub's **native issue dependencies** — the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by` (open blockers only — the live gate). Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier query**: list the map's open children (`gh issue list --state open`, scoped to the map's sub-issues / task list), drop any with an open blocker (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the `Blocked by` line) or an assignee; first in map order wins.
- **Claim**: `gh issue edit <n> --add-assignee @me` — the session's first write.
- **Resolve**: `gh issue comment <n> --body "<answer>"`, then `gh issue close <n>`, then append a context pointer (gist + link) to the map's Decisions-so-far.
