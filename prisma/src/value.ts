import { isObject } from "./narrowing";

export type PrismaValue =
  | string
  | number
  | boolean
  | null
  | undefined
  | Date
  | PrismaValue[]
  | { [key: string]: PrismaValue };

export type PrismaFilter = { [key: string]: PrismaValue };

export const isPrismaFilter = (value: PrismaValue): value is PrismaFilter =>
  isObject(value) && !Array.isArray(value) && !(value instanceof Date);
