/**
 * Example application for `@cerbos/langchain-chromadb`, run against the shared demo domain.
 *
 * This is NOT a test of what the adapter translates — `../src/adversarial.test.ts` proves that
 * against a hostile corpus with a live PDP oracle and a real ChromaDB server, and
 * `../src/translator.test.ts` pins the `Where` document each shape emits. This proves the two
 * things neither of those structurally can:
 *
 *   1. Packaging. The import below resolves through the PUBLISHED package — `exports`, `types`,
 *      the `files` allowlist, the `@cerbos/core` peer range against the copy this example
 *      declares — because run.sh installs `npm pack`'s tarball rather than linking the source
 *      directory. Both suites import from `"."` and touch none of it. See
 *      docs/adr/0002-examples-install-the-packed-artifact.md.
 *   2. Usage shape. A harness runs one flat filtered query. Consumers also paginate, and compose
 *      the adapter's filter with predicates of their own. Shape 5 below is the one that earns
 *      the exercise.
 *
 * Prints one JSON document to stdout; everything a human might want to read goes to stderr.
 * demo/scripts/run-example.sh diffs that document against demo/expected.json.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { GRPC as Cerbos } from "@cerbos/grpc";
import {
  PlanKind,
  queryPlanToChromaDB,
  type FieldMapper,
  type QueryPlanToChromaDBResult,
} from "@cerbos/langchain-chromadb";
import {
  ChromaClient,
  ChromaNotFoundError,
  type Collection,
  type Where,
} from "chromadb";

const DEMO_DIR = resolve(__dirname, "../../../demo");
const RESOURCE_KIND = "document";
const COLLECTION_NAME = "cerbos-demo-documents";

interface Seeds {
  principals: { id: string; roles: string[] }[];
  applicationFilter: { description: string; archived: boolean; region: string };
  documents: {
    id: string;
    ownerId: string;
    public: boolean;
    region: string;
    archived: boolean;
  }[];
}

// demo/seeds.json is a repository-controlled corpus file, checked structurally by
// demo/scripts/validate-demo.sh before this ever runs — not untrusted input.
const seeds = JSON.parse(
  readFileSync(resolve(DEMO_DIR, "seeds.json"), "utf8")
) as Seeds;

/**
 * Cerbos attribute names are not Chroma metadata keys, so a consumer always writes one of these.
 * Without an entry the adapter falls back to the attribute path verbatim, and
 * `request.resource.attr.ownerId` is not a key any document in this collection carries.
 *
 * `archived` and `region` are deliberately absent: they are the APPLICATION's metadata keys, never
 * referenced by demo/policies/document.yaml, and composing them with the adapter's filter is
 * shape 5. Nothing declares `required: true` either, because the demo policy never produces a
 * `$ne`/`$nin` — the operators that assertion exists to permit.
 */
const FIELD_NAME_MAPPER: FieldMapper = {
  "request.resource.attr.ownerId": "ownerId",
  "request.resource.attr.public": "public",
};

/**
 * The application's OWN predicate. Never expressed in policy; declared in demo/seeds.json.
 *
 * Spelled as an explicit `$and` rather than as two keys on one object: Chroma's validator rejects
 * a `where` carrying more than one operator ("Expected 'where' to have exactly one operator, but
 * got 2"), so the conjunction has to be written out even before anything is composed with it.
 */
const APPLICATION_FILTER: Where = {
  $and: [
    { archived: { $eq: seeds.applicationFilter.archived } },
    { region: { $eq: seeds.applicationFilter.region } },
  ],
};

/** Both addresses below are supplied, and neither has a fallback. See each in turn. */
function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `${name} is not set — run this example through demo/scripts/run-example.sh langchain-chromadb`
    );
  }
  return value;
}

/**
 * The shared runner sets this, and a fallback would be actively harmful. The obvious default —
 * Cerbos's own 3592/3593 — is the address every adapter's `cerbos run` test sidecar binds, so an
 * unset CERBOS_HOST would not fail: it would quietly plan against the conformance corpus that
 * sidecar serves, and produce a diff against demo/expected.json that reads as an adapter bug.
 * demo/README.md requires reaching the PDP at $CERBOS_HOST, "never a hardcoded address", for
 * exactly that reason.
 */
const cerbosHost = requiredEnv("CERBOS_HOST");

/**
 * Set by this example's own run.sh, which owns the ChromaDB container the way the shared runner
 * owns the PDP. No fallback here either, for a weaker but real version of the same reason: 8234 is
 * the port `npm run chroma` and this adapter's CI bind, and a default pointing at it would let a
 * demo run create and delete collections inside a conformance run's server.
 */
const chromaEndpoint = new URL(requiredEnv("CHROMA_URL"));
const chroma = new ChromaClient({
  host: chromaEndpoint.hostname,
  port: Number(chromaEndpoint.port),
});

const cerbos = new Cerbos(cerbosHost, { tls: false });

/** Exactly `PlanResourcesResponse`, without depending on a package this example never imports. */
type QueryPlan = Awaited<ReturnType<Cerbos["planResources"]>>;

function principal(id: string): { id: string; roles: string[] } {
  const found = seeds.principals.find((p) => p.id === id);
  if (!found) throw new Error(`demo/seeds.json declares no principal '${id}'`);
  return found;
}

async function plan(principalId: string, action: string): Promise<QueryPlan> {
  return cerbos.planResources({
    principal: principal(principalId),
    resource: { kind: RESOURCE_KIND },
    action,
  });
}

async function translate(
  principalId: string,
  action: string
): Promise<QueryPlanToChromaDBResult> {
  return queryPlanToChromaDB({
    queryPlan: await plan(principalId, action),
    fieldNameMapper: FIELD_NAME_MAPPER,
  });
}

/**
 * What a consumer actually writes at the call site.
 *
 * `undefined` is Chroma's word for "no metadata constraint" — every query method takes `where` as
 * an optional argument, and leaving it off matches every document. `ALWAYS_DENIED` is a string
 * sentinel rather than a second `undefined` precisely because the two must not collapse: a denial
 * that reached a query as "no constraint" would return the whole collection.
 */
type Filter = Where | undefined | "denied";

/**
 * The three plan kinds, mapped onto what Chroma's query methods accept.
 *
 * `ALWAYS_ALLOWED` needs the one line of translation in this file. The adapter returns
 * `filters: {}` for it — an empty clause, which is a faithful spelling of "no constraint" — but
 * Chroma's own validator rejects `{}` outright: `Expected 'where' to have exactly one operator,
 * but got 0`. So the caller drops the empty clause instead of forwarding it, both here and inside
 * `conjoin` below. Getting this wrong is not a silent over-grant; it is a loud `ChromaValueError`
 * on the first unconditional plan the application meets.
 *
 * A `CONDITIONAL` plan with no filter is refused rather than defaulted to "no constraint": the
 * published type makes `filters` optional across all three kinds, and reading a missing one as
 * "match everything" is exactly the over-grant this adapter throws to avoid everywhere else.
 *
 * There is no `default` arm. The three cases are every `PlanKind`, so a fourth would fail to
 * compile here rather than reaching a runtime branch nothing exercises.
 */
function toFilter(result: QueryPlanToChromaDBResult): Filter {
  switch (result.kind) {
    case PlanKind.ALWAYS_DENIED:
      return "denied";
    case PlanKind.ALWAYS_ALLOWED:
      return undefined;
    case PlanKind.CONDITIONAL: {
      const filters = result.filters;
      if (!filters || Object.keys(filters).length === 0) {
        throw new Error(
          "a KIND_CONDITIONAL plan carried no filter — refusing to query without one"
        );
      }
      return filters;
    }
  }
}

/**
 * The adapter's filter ANDed with the application's own. Chroma composes with `$and` over a list
 * of clauses, so this is one object literal — except that the list must not contain the empty
 * clause an unconditional plan produces, which is the whole reason `undefined` is handled first
 * rather than being wrapped like any other operand.
 */
function conjoin(
  adapterFilter: Where | undefined,
  applicationFilter: Where
): Where {
  if (adapterFilter === undefined) return applicationFilter;
  return { $and: [adapterFilter, applicationFilter] };
}

/**
 * A deterministic vector per document.
 *
 * The demo domain is about metadata filtering rather than similarity ranking, so nothing below
 * depends on the distances — but a collection holding one embedding eight times over would not be
 * exercising the query path a consumer's `where` clause actually travels.
 */
function embeddingFor(index: number): number[] {
  return [0.1, 0.2, 0.3, index / 100];
}

/** The vector every search below is run from — the first document's own, so it names no constant
 * the seeding does not already use. */
const QUERY_EMBEDDING = embeddingFor(0);

/**
 * A similarity search with the filter attached — `collection.query` is what LangChain's Chroma
 * vector store calls underneath `similaritySearch`, and `where` is the argument it forwards.
 *
 * `nResults` is Chroma's cap on how many neighbours come back, so "no limit" has to be spelled as
 * the collection size; shape 4 is what varies it. The returned ids are sorted before they are
 * asserted, because a vector search ranks by distance and demo/expected.json is shared by stores
 * that have no notion of one.
 */
async function findIds(
  collection: Collection,
  filter: Filter
): Promise<string[]> {
  if (filter === "denied") return [];
  const results = await collection.query({
    queryEmbeddings: [QUERY_EMBEDDING],
    where: filter,
    nResults: seeds.documents.length,
  });
  return [...(results.ids[0] ?? [])].sort();
}

interface ShapeResult {
  kind: PlanKind;
  ids: string[];
}

interface PaginatedShapeResult extends ShapeResult {
  pageSize: number;
  pageSizes: number[];
}

// ---------------------------------------------------------------------------------------------
// The five usage shapes
// ---------------------------------------------------------------------------------------------

/** Shapes 1, 2 and 3: a plain filtered list. The adapter's filter is the whole query. */
async function filtered(
  collection: Collection,
  principalId: string,
  action: string
): Promise<ShapeResult> {
  const result = await translate(principalId, action);
  return { kind: result.kind, ids: await findIds(collection, toFilter(result)) };
}

/**
 * Shape 4: a limit applied on top of the filter, walked to the end of the result set.
 *
 * This is the one shape that leaves `collection.query` behind. A vector search takes `nResults` —
 * a limit — but has no offset, so it can return the first page and no other; the second page needs
 * `collection.get`, Chroma's metadata-only retrieval path, which takes the same `where` clause plus
 * `limit` and `offset`. Both halves are worth exercising: the filter this adapter emits has to be
 * accepted by whichever of the two a consumer reaches for.
 *
 * Reported as page SIZES plus the sorted union of the ids, never as per-page order —
 * demo/expected.json is shared by every store and several of them have no total order to paginate
 * by. Disjointness still falls out: overlapping pages would shrink the union below the sum.
 *
 * There is no ordering argument to add, unlike the SQL-backed examples, which sort by id so that
 * limit/offset cannot repeat or omit a row between pages. `collection.get` takes `limit` and
 * `offset` and nothing to order by, so the pages arrive in whatever order the store walks the
 * collection in. That is enough for what is asserted here, and it is the assertion rather than a
 * documented guarantee that makes it enough: a walk that reordered between two calls would show up
 * as a union shorter than the sum of the page sizes, which is a failure, not a silent pass.
 */
async function paginated(
  collection: Collection,
  principalId: string,
  action: string,
  pageSize: number
): Promise<PaginatedShapeResult> {
  const result = await translate(principalId, action);
  const filter = toFilter(result);

  const pageSizes: number[] = [];
  const ids: string[] = [];
  if (filter !== "denied") {
    for (let offset = 0; ; offset += pageSize) {
      const page = await collection.get({
        where: filter,
        limit: pageSize,
        offset,
      });
      if (page.ids.length === 0) break;
      pageSizes.push(page.ids.length);
      ids.push(...page.ids);
      if (page.ids.length < pageSize) break;
    }
  }

  return { kind: result.kind, pageSize, pageSizes, ids: ids.sort() };
}

/**
 * Shape 5: the adapter's filter ANDed with the application's own predicate. All three plan kinds
 * go through here on purpose — an `ALWAYS_ALLOWED` plan has no clause to AND with, and an
 * `ALWAYS_DENIED` one must not have its denial undone by the application's predicate.
 */
async function composed(
  collection: Collection,
  principalId: string,
  action: string
): Promise<ShapeResult> {
  const result = await translate(principalId, action);
  const filter = toFilter(result);
  if (filter === "denied") {
    return { kind: result.kind, ids: [] };
  }
  return {
    kind: result.kind,
    ids: await findIds(collection, conjoin(filter, APPLICATION_FILTER)),
  };
}

// ---------------------------------------------------------------------------------------------
// The collection
// ---------------------------------------------------------------------------------------------

async function seed(): Promise<Collection> {
  try {
    await chroma.deleteCollection({ name: COLLECTION_NAME });
  } catch (error: unknown) {
    if (!(error instanceof ChromaNotFoundError)) throw error;
  }

  // `embeddingFunction: null` because this example supplies its own vectors. Left unset, the
  // client reaches for @chroma-core/default-embed, which is not installed and which an example
  // proving packaging has no business downloading a model for.
  const collection = await chroma.createCollection({
    name: COLLECTION_NAME,
    embeddingFunction: null,
  });

  await collection.add({
    ids: seeds.documents.map(({ id }) => id),
    embeddings: seeds.documents.map((_, index) => embeddingFor(index)),
    // Two of these four keys are what the policy's conditions resolve to through the mapper above;
    // the other two are the application's, and exist only to be composed with in shape 5.
    metadatas: seeds.documents.map((document) => ({
      ownerId: document.ownerId,
      public: document.public,
      region: document.region,
      archived: document.archived,
    })),
    documents: seeds.documents.map(
      (document) => `Document ${document.id}, owned by ${document.ownerId}.`
    ),
  });

  return collection;
}

async function main(): Promise<void> {
  const collection = await seed();
  console.error(`seeded ${seeds.documents.length} documents`);

  const shapes = {
    filtered: {
      "alice/view": await filtered(collection, "alice", "view"),
      "bob/view": await filtered(collection, "bob", "view"),
    },
    alwaysAllowed: {
      "admin/admin-view": await filtered(collection, "admin", "admin-view"),
    },
    alwaysDenied: {
      "alice/publish": await filtered(collection, "alice", "publish"),
    },
    paginated: {
      "alice/view": await paginated(collection, "alice", "view", 2),
      "admin/admin-view": await paginated(collection, "admin", "admin-view", 3),
    },
    composed: {
      "alice/view": await composed(collection, "alice", "view"),
      "bob/view": await composed(collection, "bob", "view"),
      "admin/admin-view": await composed(collection, "admin", "admin-view"),
      "alice/publish": await composed(collection, "alice", "publish"),
    },
  };

  process.stdout.write(
    `${JSON.stringify({ adapter: "langchain-chromadb", shapes }, null, 2)}\n`
  );
}

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
})();
