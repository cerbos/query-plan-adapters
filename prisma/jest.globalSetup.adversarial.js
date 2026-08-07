const { execFileSync } = require("node:child_process");

/**
 * Bring up the store the adversarial suite replays the corpus against.
 *
 * SQLite needs nothing here: `npm run test:adversarial:v*` pushes `prisma/dev-adversarial.db`
 * before jest starts, exactly as it always did. The PostgreSQL leg
 * (cerbos/query-plan-adapters#320) cannot work that way — the connection string only exists once
 * the container is running — so the container is started here, `DATABASE_URL` is exported for the
 * test workers jest forks next, and `prisma db push` creates the schema against it.
 *
 * `prisma db push` rather than raw DDL on purpose: the columns are the point of this leg (a real
 * `integer`, a real `timestamp(3)`), and only Prisma's own migration engine renders the model the
 * client will then query through.
 */
module.exports = async function globalSetup() {
  if ((process.env.ADAPTER_TEST_DB ?? "sqlite") !== "postgres") {
    return;
  }

  // Mirrors the ent harness's PostgreSQL target so both adapters prove the same server.
  const image = "postgres:17-alpine";
  const { PostgreSqlContainer } = require("@testcontainers/postgresql");
  const container = await new PostgreSqlContainer(image).start();

  process.env.DATABASE_URL = container.getConnectionUri();
  // globalTeardown runs in its own module registry, so the handle travels through globalThis.
  globalThis.__ADVERSARIAL_POSTGRES__ = container;

  // The two majors differ in every part of the invocation, so they are described once rather
  // than branched on three times. Prisma 6's `db push` has no `--url` and reads the schema's
  // `env("DATABASE_URL")` (set above); Prisma 7 dropped `--skip-generate` and takes the url on
  // the command line, its adversarial schema carrying none.
  const majors = {
    6: {
      cli: "node_modules/prisma-v6/build/index.js",
      schema: "prisma/schema.adversarial.pg.v6.prisma",
      extraArgs: () => ["--skip-generate"],
    },
    7: {
      cli: "node_modules/prisma/build/index.js",
      schema: "prisma/schema.adversarial.pg.prisma",
      extraArgs: () => [`--url=${process.env.DATABASE_URL}`],
    },
  };

  const prismaVersion = process.env.PRISMA_VERSION || "7";
  const major = majors[prismaVersion];
  if (!major) {
    throw new Error(
      `Unknown PRISMA_VERSION "${prismaVersion}": expected one of ${Object.keys(majors).join(", ")}`
    );
  }

  execFileSync(
    process.execPath,
    [
      major.cli,
      "db",
      "push",
      "--force-reset",
      `--schema=${major.schema}`,
      ...major.extraArgs(),
    ],
    { stdio: "inherit", env: process.env }
  );
};
