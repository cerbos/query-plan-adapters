import { UnsupportedQueryPlanError } from "./errors";

/** Escapes every regex metacharacter so the value matches itself literally. */
export const escapeRegexValue = (value: string): string =>
  value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Rewrites a CEL (RE2) pattern into the subset RE2 and MongoDB's PCRE2 read identically, and
 * refuses anything outside it: literal escapes, `^` only at the start, `$` only at the end (as
 * `\z`, because PCRE2's `$` also matches before a final newline), and the `*`/`+`/`?` quantifiers.
 */
export const normalizeRe2PatternForMongo = (pattern: string): string => {
  const escapedLiterals = new Set("\\.^$*+?()[]{}|".split(""));
  const unsupportedSyntax = new Set("()[]{}|".split(""));
  let normalized = "";
  let canQuantify = false;

  for (let index = 0; index < pattern.length; index += 1) {
    const character = pattern[index];
    if (!character) {
      break;
    }
    if (character === "\\") {
      const escaped = pattern[index + 1];
      if (!escaped || !escapedLiterals.has(escaped)) {
        throw new UnsupportedQueryPlanError(
          "matches supports only literal escapes in the common RE2/PCRE2 subset",
        );
      }
      normalized += `\\${escaped}`;
      canQuantify = true;
      index += 1;
      continue;
    }
    if (character === "^") {
      if (index !== 0) {
        throw new UnsupportedQueryPlanError("matches supports ^ only at the start of the pattern");
      }
      normalized += character;
      canQuantify = false;
      continue;
    }
    if (character === "$") {
      if (index !== pattern.length - 1) {
        throw new UnsupportedQueryPlanError("matches supports $ only at the end of the pattern");
      }
      normalized += "\\z";
      canQuantify = false;
      continue;
    }
    if (character === "*" || character === "+" || character === "?") {
      if (!canQuantify) {
        throw new UnsupportedQueryPlanError(`matches has an invalid ${character} quantifier`);
      }
      normalized += character;
      canQuantify = false;
      continue;
    }
    if (unsupportedSyntax.has(character) || character.charCodeAt(0) < 0x20) {
      throw new UnsupportedQueryPlanError(
        "matches pattern is outside the supported common RE2/PCRE2 subset",
      );
    }
    normalized += character;
    canQuantify = true;
  }

  return normalized;
};
