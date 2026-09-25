/**
 * CEL's `string()` over a double, reproduced exactly so a comparison against its result can be
 * inverted into a numeric comparison against the column (see `buildNumberStringComparison`).
 *
 * cel-go spells the conversion `fmt.Sprintf("%g", d)`: Go's shortest round-trip digits — the same
 * digits JavaScript's `toExponential()` picks — laid out in exponent form when the decimal
 * exponent is below -4 or at least 6, with a sign and at least two exponent digits (`1e+06`,
 * `1.5e-05`), and in positional form otherwise (`123456`, `0.0001`). Checked against Go's own
 * output for twenty thousand doubles, not inferred from its documentation.
 */
export const formatCelDouble = (value: number): string => {
  if (Number.isNaN(value)) return "NaN";
  if (value === Infinity) return "+Inf";
  if (value === -Infinity) return "-Inf";
  const sign = value < 0 || Object.is(value, -0) ? "-" : "";
  const [mantissa, exponentText] = Math.abs(value).toExponential().split("e");
  const digits = mantissa!.replace(".", "");
  const exponent = Number(exponentText);
  if (exponent < -4 || exponent >= 6) {
    const fraction = digits.length > 1 ? `.${digits.slice(1)}` : "";
    const magnitude = String(Math.abs(exponent)).padStart(2, "0");
    return `${sign}${digits[0]}${fraction}e${exponent < 0 ? "-" : "+"}${magnitude}`;
  }
  if (exponent < 0) {
    return `${sign}0.${"0".repeat(-exponent - 1)}${digits}`;
  }
  const integerDigits = exponent + 1;
  return digits.length <= integerDigits
    ? `${sign}${digits.padEnd(integerDigits, "0")}`
    : `${sign}${digits.slice(0, integerDigits)}.${digits.slice(integerDigits)}`;
};

/**
 * The one double whose CEL `string()` is exactly `text`, or `undefined` when no double's is —
 * `"2.0"`, `"1e6"` and `" 2"` all parse, but CEL spells those values `"2"` and `"1e+06"`, so no
 * row's `string()` can ever equal them.
 */
export const parseCelDoubleString = (text: string): number | undefined => {
  const value = Number(text);
  return text.trim() !== "" && formatCelDouble(value) === text ? value : undefined;
};
