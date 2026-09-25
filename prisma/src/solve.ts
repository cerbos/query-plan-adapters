// Solving `f(column) CMP v` for a monotone IEEE-754 function f exactly, over the doubles.

/**
 * The doubles `x` for which a comparison holds, as an interval of finite doubles: `lo`/`hi` are
 * inclusive bounds, undefined when that side is unbounded. `empty` when no double satisfies it.
 */
export type DoubleInterval =
  | { empty: true }
  | { empty: false; lo: number | undefined; hi: number | undefined };

const view = new DataView(new ArrayBuffer(8));
const SIGN = 1n << 63n;

/** An integer key that orders finite doubles as the doubles themselves (+0 and -0 share one). */
function toKey(x: number): bigint {
  view.setFloat64(0, x);
  const bits = view.getBigUint64(0);
  return bits & SIGN ? -(bits & ~SIGN) : bits;
}

function fromKey(key: bigint): number {
  view.setBigUint64(0, key < 0n ? -key | SIGN : key);
  return view.getFloat64(0);
}

const MIN_KEY = toKey(-Number.MAX_VALUE);
const MAX_KEY = toKey(Number.MAX_VALUE);

/** The least key in [MIN_KEY, MAX_KEY] whose double satisfies `holds`, or undefined. */
function leastKey(holds: (x: number) => boolean): bigint | undefined {
  if (!holds(fromKey(MAX_KEY))) return undefined;
  let low = MIN_KEY;
  let high = MAX_KEY;
  while (low < high) {
    const mid = low + (high - low) / 2n;
    if (holds(fromKey(mid))) high = mid;
    else low = mid + 1n;
  }
  return low;
}

/**
 * The doubles `x` with `f(x) CMP v`, for `f` non-decreasing over the finite doubles — IEEE-754
 * addition and multiplication by a positive constant are, whatever rounding does to them, which
 * is what makes this exact where subtracting the constant from `v` is not.
 */
export function solveMonotone(
  f: (x: number) => number,
  operator: "eq" | "lt" | "le" | "gt" | "ge",
  v: number
): DoubleInterval {
  // {x : f(x) >= v} and {x : f(x) > v} are both upward-closed.
  const atLeast = leastKey((x) => f(x) >= v);
  const above = leastKey((x) => f(x) > v);
  const interval = (lo: bigint | undefined, hi: bigint | undefined): DoubleInterval => {
    if (lo !== undefined && hi !== undefined && lo > hi) return { empty: true };
    return {
      empty: false,
      lo: lo === undefined || lo === MIN_KEY ? undefined : fromKey(lo),
      hi: hi === undefined || hi === MAX_KEY ? undefined : fromKey(hi),
    };
  };
  const below = (key: bigint | undefined) => (key === undefined ? MAX_KEY : key - 1n);
  switch (operator) {
    case "ge":
      return atLeast === undefined ? { empty: true } : interval(atLeast, undefined);
    case "gt":
      return above === undefined ? { empty: true } : interval(above, undefined);
    case "le":
      return above === MIN_KEY ? { empty: true } : interval(undefined, below(above));
    case "lt":
      return atLeast === MIN_KEY ? { empty: true } : interval(undefined, below(atLeast));
    case "eq":
      return atLeast === undefined ? { empty: true } : interval(atLeast, below(above));
  }
}

/** The interval of `-x` for `x` in `interval`. */
export function negateInterval(interval: DoubleInterval): DoubleInterval {
  if (interval.empty) return interval;
  return {
    empty: false,
    lo: interval.hi === undefined ? undefined : -interval.hi,
    hi: interval.lo === undefined ? undefined : -interval.lo,
  };
}
