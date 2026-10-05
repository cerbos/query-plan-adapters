# CI and releases

How the GitHub Actions workflows are laid out, and why. Read this before editing anything under
`.github/workflows/` or cutting a release.

Each adapter has its own GitHub Actions workflow triggered by changes in its directory or `/conformance/` — plus `/demo/` where that adapter has an example. Every one of those workflows, `conformance.yaml` included, ends its `paths` with `!**/*.md`: no script, harness or build reads Markdown, so a prose-only change runs no CI. Keep that negation last in any new workflow, since a later positive pattern re-includes what it excluded, and drop it for good if a script ever starts reading a Markdown file.

Every adapter workflow runs `validate-corpus.sh` and its conformance harness **inside the same job as the regular tests**, and no adapter workflow starts a PDP. Convex is the one exception to the single job, and not by choice: its harness imports `convex/_generated`, which only exists once a live backend has been deployed to, so the corpus leg lives in the job that does the deploy and the codegen. On the TypeScript adapters the harness is gated to the baseline Node leg (`if: matrix.node-version == '22'`), because the corpus discriminates the translator and the datastore, not the Node runtime. The other matrix dimensions divide into two kinds:

- **The datastore is one.** Drizzle, Prisma and ActiveRecord run the corpus once per `ADAPTER_TEST_DB` store (SQLite, PostgreSQL, MySQL), and SQLAlchemy's harness runs all three in one `pdm run test` — collation, LIKE escaping, cast targets and parameter typing are translator behaviour, so a store the workflow does not execute is a store the adapter does not cover. MongoDB server version is the mongoose equivalent, and it exists only on the baseline Node leg.
- **The client engine is not, on its own.** Prisma's v6/v7 dimension crosses with the store dimension, giving six conformance runs per Prisma workflow, all on Node 22. Spring-data's ORM set (`baseline`, `next`), Exposed's (`baseline`, `floor`) and ActiveRecord's version (8.0, 7.1) cross with their store dimensions the same way, since each renders the SQL the store executes.

Adding a store leg buys coverage; adding a Node leg does not. The PDP is not a dimension of any adapter workflow: every harness replays both pinned PDPs' goldens in one run. `conformance.yaml` is the only workflow that starts a PDP: it runs `validate-corpus.sh`, `verify-cerbos-digest.sh`, vets and `gofmt`-checks the generator, and runs `go -C conformance/generator run . -check`.

Every PR-triggered workflow declares a `concurrency` group that cancels a pull request's superseded run; give a new workflow the same block. Adapter workflows never run on `main`, and a cache written from a pull request is visible to that pull request alone, so `warm-caches.yaml` writes the npm, Go and Gradle caches on `main` for every pull request to restore. It can only do that under the keys the jobs look up, so a job's `cache-dependency-path` (and, for Go, its `go-version-file`) must match the entry in `warm-caches.yaml` — the generator's included. Gradle jobs cache through `setup-java`'s `cache: gradle` with `setup-gradle`'s own cache disabled, because `setup-gradle` keys its entries by job id and writes them only on `main`.

Npm releases use `<package-name>@v<version>` tags (for example, `@cerbos/orm-prisma@v5.0.0`),
as declared in each `*-publish.yaml` workflow. The publish workflow calls the adapter's test
workflow at the tagged commit and publishes only after its full matrix, conformance suite and
packaged example succeed. Adapter test workflows run directly on pull requests and through
`workflow_call` for releases, so a release runs the checks once. Keep the publish workflow
filenames stable: npm trusted publishing is configured against them.

Other release tags: `sqla/v*` -> PyPI, `activerecord/v*` -> RubyGems; `ent/v*` and `pgx/v*` are Go
module tags resolved directly from the repository. `elasticsearch-java/v*`, `spring-data/v*` and `exposed/v*` only run that
adapter's CI workflow: none of those builds configures a Maven Central release (all three are `publishToMavenLocal` only, and
their `publishing` blocks say what wiring a release still needs), so no Maven Central publish is wired yet.
