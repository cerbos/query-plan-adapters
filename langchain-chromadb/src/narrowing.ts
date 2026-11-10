import type { Value } from "@cerbos/core";

export const isString = <T>(value: T): value is T & string => typeof value === "string";

export const isNumber = <T>(value: T): value is T & number => typeof value === "number";

export const isBoolean = <T>(value: T): value is T & boolean => typeof value === "boolean";

export const isBigint = <T>(value: T): value is T & bigint => typeof value === "bigint";

export const isTypeofObject = <T>(value: T): value is T & (object | null) => typeof value === "object";

export const isFunction = <T>(value: T): value is Extract<T, Function> => typeof value === "function";

export const isObject = <T>(value: T): value is T & object => typeof value === "object" && value !== null;

export const typeofValue = (value: Value): "string" | "number" | "boolean" | "object" =>
  isString(value) ? "string" : isNumber(value) ? "number" : isBoolean(value) ? "boolean" : "object";

const hasKey = <T extends object>(table: T, key: PropertyKey): key is keyof T => Object.hasOwn(table, key);

export const lookup = <T extends object>(table: T, key: PropertyKey): T[keyof T] | undefined =>
  hasKey(table, key) ? table[key] : undefined;

const missingOperand = (): never => {
  throw new Error("An expression is missing an operand");
};

export const single = <T>([a]: readonly T[]): [T] => [a ?? missingOperand()];

export const pair = <T>([a, b]: readonly T[]): [T, T] => [a ?? missingOperand(), b ?? missingOperand()];

export const triple = <T>([a, b, c]: readonly T[]): [T, T, T] => [
  a ?? missingOperand(),
  b ?? missingOperand(),
  c ?? missingOperand(),
];
