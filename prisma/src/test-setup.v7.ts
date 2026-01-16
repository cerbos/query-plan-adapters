import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import { Prisma, PrismaClient } from "./generated/prisma/client";

const adapter = new PrismaBetterSqlite3({ url: "./prisma/dev.db" });
const prisma = new PrismaClient({ adapter });

export { prisma, Prisma };
