import { Prisma, PrismaClient } from "./generated/prisma-adversarial-mysql-v6";

import { adversarialDatabaseUrl } from "./test-setup.adversarial-url";

// Prisma 6 reads the datasource URL from the schema's `env("DATABASE_URL")`, which the jest
// globalSetup exports once the container is up. Passing it explicitly as well keeps the failure
// mode loud: `adversarialDatabaseUrl` throws when the variable is missing, rather than letting the
// client fall back to whatever a developer happens to have in their environment.
const prisma = new PrismaClient({
  datasources: { db: { url: adversarialDatabaseUrl() } },
});

export { prisma, Prisma };
