import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import { datamodel } from "./generated/cerbos-datamodel/datamodel";
import {
  Prisma,
  PrismaClient,
} from "./generated/prisma-adversarial/client";

const adapter = new PrismaBetterSqlite3({
  url: "./prisma/dev-adversarial.db",
});

const prisma = new PrismaClient({ adapter });

export { prisma, Prisma, datamodel };
