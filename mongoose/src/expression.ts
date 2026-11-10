import { isObject } from "./narrowing";

export type MongoValue =
  | string
  | number
  | boolean
  | null
  | undefined
  | Date
  | RegExp
  | MongoValue[]
  | { [key: string]: MongoValue };

export type MongooseFilter = { [key: string]: MongoValue };

export const isMongoDocument = (value: MongoValue): value is MongooseFilter =>
  isObject(value) && !Array.isArray(value) && !(value instanceof Date) && !(value instanceof RegExp);
