import { Prisma, PrismaClient } from "./generated/prisma-adversarial-v6";

const prisma = new PrismaClient();

export { prisma, Prisma };
