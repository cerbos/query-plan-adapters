/**
 * The PostgreSQL connection string the adversarial harness's container leg runs against.
 *
 * `jest.globalSetup.adversarial.js` starts the container and exports `DATABASE_URL` before any
 * test module loads. Throwing when it is missing is the point: a client that silently connected
 * somewhere else — a developer's local PostgreSQL, an empty SQLite file — would replay the corpus
 * against rows nobody seeded and report a passing differential over an empty store.
 */
export function adversarialDatabaseUrl(): string {
  const url = process.env["DATABASE_URL"];
  if (!url) {
    throw new Error(
      "DATABASE_URL is not set: the PostgreSQL adversarial leg is started by " +
        "jest.globalSetup.adversarial.js, so run it through `npm run test:adversarial:postgres`"
    );
  }
  return url;
}
