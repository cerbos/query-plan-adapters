import { Prisma, PrismaClient } from "./generated/prisma-adversarial-v5";

const prisma = new PrismaClient();

export { prisma, Prisma };
