import { PrismaMariaDb } from "@prisma/adapter-mariadb";
import { Prisma, PrismaClient } from "./generated/prisma-adversarial-mysql/client";

import { adversarialDatabaseUrl } from "./test-setup.adversarial-url";

// Prisma 7 reaches a database only through a driver adapter, and `@prisma/adapter-mariadb` is the
// one Prisma ships for the MySQL provider — the MariaDB connector speaks the MySQL protocol,
// including MySQL 8.4's caching_sha2_password default.
const adapter = new PrismaMariaDb(adversarialDatabaseUrl());
const prisma = new PrismaClient({ adapter });

export { prisma, Prisma };
