import { Prisma, PrismaClient } from "./generated/prisma-adversarial-v6";

const prisma = new PrismaClient();

const datamodel = Prisma.dmmf.datamodel;

export { prisma, Prisma, datamodel };
