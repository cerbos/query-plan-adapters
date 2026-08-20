const { execFileSync } = require("node:child_process");

/**
 * Bring up the store the adversarial suite replays the corpus against.
 *
 * SQLite needs nothing here: `npm run test:adversarial:v*` pushes `prisma/dev-adversarial.db`
 * before jest starts, exactly as it always did. A container-backed leg cannot work that way — the
 * connection string only exists once the container is running — so the container is started here,
 * `DATABASE_URL` is exported for the test workers jest forks next, and `prisma db push` creates
 * the schema against it. PostgreSQL was the first (cerbos/query-plan-adapters#320), MySQL the
 * second (#340).
 *
 * `prisma db push` rather than raw DDL on purpose: the columns are the point of these legs (a real
 * `integer`, a real `timestamp(3)`/`datetime(3)`), and only Prisma's own migration engine renders
 * the model the client will then query through.
 *
 * It is also why the MySQL leg's collation is applied AFTER the push rather than configured on the
 * server: see `MYSQL_COLLATION` below.
 */

// Both images mirror the ent and spring-data harnesses' targets so every adapter proves the same
// servers. Pinned by tag AND digest: a tag is mutable, so a tag-only pin records an intent rather
// than a build, and these legs exist to prove typed-column and collation behaviour a re-pushed
// image could change under them. `conformance/scripts/validate-corpus.sh` asserts every service
// image reference in the repository carries both halves.
const POSTGRES_IMAGE =
  "postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193";
const MYSQL_IMAGE =
  "mysql:8.4@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb";

/**
 * The collation the MySQL leg runs the whole corpus under.
 *
 * CEL string equality is byte-exact. MySQL's own default `utf8mb4_0900_ai_ci` is case- AND
 * accent-insensitive and its `LIKE` follows the column's collation — but the server's default is
 * not even what a Prisma-managed database gets. **Prisma's MySQL migration engine hardcodes
 * `DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci` into every `CREATE TABLE` and ignores
 * the server's default entirely**, and `utf8mb4_unicode_ci` is case- and accent-insensitive too.
 * Measured against MySQL 8.4 with `prisma db push`, not inferred — which is why this leg
 * converts the tables afterwards instead of passing `--collation-server=…` to the container, a
 * flag that would read as doing something and do nothing.
 *
 * Under either insensitive collation `'One' = 'one'` is TRUE, and `cs-eq`, `unicode-eq` and every
 * `hier-*` prefix probe return rows the PDP denies. That is a store misconfiguration rather than a
 * limitation of this adapter — no Prisma `where` clause could restore byte-exact equality — so the
 * leg pins a case- and accent-sensitive collation and states the requirement, as `spring-data`'s
 * MySQL leg does. `README.md` says the same thing to consumers, who inherit the same default.
 *
 * `utf8mb4_0900_as_cs` rather than the older `utf8mb4_bin` that `ent`'s DDL names: the latter is
 * PAD SPACE, so `'a' = 'a '` is TRUE under it, while `_0900_as_cs` is NO PAD and so matches CEL on
 * that axis as well. It is also the collation `spring-data`'s MySQL leg already runs.
 *
 * Overridable so the over-grant can be reproduced rather than taken on trust:
 * `ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_ai_ci npm run test:adversarial:mysql` fails on the
 * case and accent probes. Same escape hatch as spring-data's `-Dadapter.test.mysql.collation`.
 */
const MYSQL_COLLATION =
  process.env.ADAPTER_TEST_MYSQL_COLLATION ?? "utf8mb4_0900_as_cs";

/**
 * Give every table Prisma just created the collation above.
 *
 * On its own connection, and with foreign-key checks off for the duration: a converted table
 * momentarily disagrees with the tables still pointing at it, and MySQL rejects that outright
 * (`ERROR 3780`). `foreign_key_checks` is a SESSION variable, so this cannot run through Prisma's
 * pool, where the next statement may land on another connection.
 *
 * Read from `information_schema` rather than from a list of model names, so a model added to the
 * schema is converted too instead of being silently left case-insensitive.
 */
async function applyMysqlCollation(url) {
  const mariadb = require("mariadb");
  // Spelled out rather than handed the URL: the connector's own parser accepts only a
  // `mariadb://` scheme, and testcontainers hands back a `mysql://` one.
  const { hostname, port, username, password, pathname } = new URL(url);
  const connection = await mariadb.createConnection({
    host: hostname,
    port: Number(port),
    user: decodeURIComponent(username),
    password: decodeURIComponent(password),
    database: decodeURIComponent(pathname.replace(/^\//, "")),
  });
  try {
    const tables = await connection.query(
      "select TABLE_NAME as name from information_schema.TABLES where TABLE_SCHEMA = database()"
    );
    // An empty list would leave every conversion unexecuted and the whole leg running on Prisma's
    // case-insensitive default while reporting nothing.
    if (tables.length === 0) {
      throw new Error(
        "prisma db push created no tables: the MySQL collation conversion would guard nothing"
      );
    }
    await connection.query("SET FOREIGN_KEY_CHECKS = 0");
    for (const { name } of tables) {
      await connection.query(
        `ALTER TABLE \`${name}\` CONVERT TO CHARACTER SET utf8mb4 COLLATE ${MYSQL_COLLATION}`
      );
    }
    await connection.query("SET FOREIGN_KEY_CHECKS = 1");
  } finally {
    await connection.end();
  }
}

/**
 * One entry per container-backed store: how to start it, which schema each Prisma major pushes
 * into it, and anything the store needs doing to it once the schema is there.
 */
const CONTAINER_STORES = {
  postgres: {
    async start() {
      const { PostgreSqlContainer } = require("@testcontainers/postgresql");
      return new PostgreSqlContainer(POSTGRES_IMAGE).start();
    },
    schemas: {
      5: "prisma/schema.adversarial.pg.v5.prisma",
      6: "prisma/schema.adversarial.pg.v6.prisma",
      7: "prisma/schema.adversarial.pg.prisma",
    },
  },
  mysql: {
    async start() {
      const { MySqlContainer } = require("@testcontainers/mysql");
      // Stock server settings: see MYSQL_COLLATION above for why configuring the server here
      // would prove nothing.
      return new MySqlContainer(MYSQL_IMAGE).start();
    },
    schemas: {
      5: "prisma/schema.adversarial.mysql.v5.prisma",
      6: "prisma/schema.adversarial.mysql.v6.prisma",
      7: "prisma/schema.adversarial.mysql.prisma",
    },
    afterPush: applyMysqlCollation,
  },
};

// The legacy and current clients differ in every part of the `db push` invocation, so they are
// described once rather than branched on per store. Prisma 5 and 6 have no `--url` and read the
// schema's `env("DATABASE_URL")` (set below); Prisma 7 dropped `--skip-generate` and takes the url
// on the command line, its adversarial schemas carrying none.
const MAJORS = {
  5: {
    cli: "node_modules/prisma-v5/build/index.js",
    extraArgs: () => ["--skip-generate"],
  },
  6: {
    cli: "node_modules/prisma-v6/build/index.js",
    extraArgs: () => ["--skip-generate"],
  },
  7: {
    cli: "node_modules/prisma/build/index.js",
    extraArgs: () => [`--url=${process.env.DATABASE_URL}`],
  },
};

module.exports = async function globalSetup() {
  const storeName = process.env.ADAPTER_TEST_DB ?? "sqlite";
  const store = CONTAINER_STORES[storeName];
  if (!store) {
    return;
  }

  const container = await store.start();
  process.env.DATABASE_URL = container.getConnectionUri();
  // globalTeardown runs in its own module registry, so the handle travels through globalThis.
  globalThis.__ADVERSARIAL_STORE_CONTAINER__ = container;

  const prismaVersion = process.env.PRISMA_VERSION || "7";
  const major = MAJORS[prismaVersion];
  if (!major) {
    throw new Error(
      `Unknown PRISMA_VERSION "${prismaVersion}": expected one of ${Object.keys(MAJORS).join(", ")}`
    );
  }

  // No `--force-reset` here, unlike the SQLite leg in package.json: that leg pushes into
  // `prisma/dev-adversarial.db`, a file which survives between runs and has to be dropped. This
  // database was created by the container a few seconds ago and is empty, so the reset is a no-op
  // — and a destructive flag that does nothing is one Prisma Migrate refuses to run under an AI
  // agent, which would otherwise make these legs the only ones a contributor cannot reproduce
  // exactly as CI runs them.
  execFileSync(
    process.execPath,
    [
      major.cli,
      "db",
      "push",
      `--schema=${store.schemas[prismaVersion]}`,
      ...major.extraArgs(),
    ],
    { stdio: "inherit", env: process.env }
  );

  await store.afterPush?.(process.env.DATABASE_URL);
};
