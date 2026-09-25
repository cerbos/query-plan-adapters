// Copied from the convex adapter rather than shared: adapters share data, not code
// (docs/adr/0007-adapters-share-data-not-code.md).

/**
 * The subset of RE2 the post-filter answers: a literal of plain characters, optionally anchored at
 * either end or followed by a trailing `.*`. The post-filter answers it with plain string
 * comparisons rather than a regex engine, and any other pattern is refused at translation time.
 */
export interface SafeRegexPattern {
  literal: string;
  anchoredStart: boolean;
  anchoredEnd: boolean;
  trailingWildcard: boolean;
}

const SAFE_REGEX_PATTERN = /^(\^)?([A-Za-z0-9 _:/-]+?)(\.\*)?(\$)?$/;

export const parseSafeRegexPattern = (
  pattern: string,
): SafeRegexPattern | undefined => {
  const match = SAFE_REGEX_PATTERN.exec(pattern);
  const literal = match?.[2];
  if (!literal) return undefined;
  if (match[3] === ".*" && match[4] === "$") return undefined;
  return {
    literal,
    anchoredStart: match[1] === "^",
    trailingWildcard: match[3] === ".*",
    anchoredEnd: match[4] === "$",
  };
};

export const matchesSafeRegexPattern = (
  receiver: string,
  pattern: SafeRegexPattern,
): boolean => {
  if (pattern.anchoredStart && pattern.anchoredEnd) {
    return receiver === pattern.literal;
  }
  if (pattern.anchoredStart) return receiver.startsWith(pattern.literal);
  if (pattern.anchoredEnd) return receiver.endsWith(pattern.literal);
  return receiver.includes(pattern.literal);
};
