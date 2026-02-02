import { Prisma, PrismaClient } from "./generated/prisma-v6";

const prisma = new PrismaClient();

export { prisma, Prisma };
