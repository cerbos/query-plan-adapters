import { PrismaPg } from "@prisma/adapter-pg";
import { Prisma, PrismaClient } from "./generated/prisma-adversarial-pg/client";

import { adversarialDatabaseUrl } from "./test-setup.adversarial-url";

const adapter = new PrismaPg({ connectionString: adversarialDatabaseUrl() });
const prisma = new PrismaClient({ adapter });

export { prisma, Prisma };
