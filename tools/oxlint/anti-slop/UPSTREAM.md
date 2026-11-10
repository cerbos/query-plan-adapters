# Upstream

- Source: https://github.com/dmmulroy/anti-slop
- Commit: 13909c95 (`src/` copied by `skills/install-anti-slop/scripts/install.mjs` at that commit)
- Installed at: `tools/oxlint/anti-slop/`, registered by `tools/oxlint/oxlint.config.ts`
- Deviations: none in the vendored source. The Effect rules are vendored but not registered: no
  adapter depends on Effect.
- Configuration (`tools/oxlint/oxlint.config.ts`): every generic rule at `error`. oxlint's default
  `correctness` category is off, because its autofix rewrote `[...string].some` into
  `string.some`. `no-runtime-typeof` takes `allowInTypeGuards: true`, so each adapter's
  `src/narrowing.ts` holds the `typeof` checks as type predicates. `no-shape-in-symbol-names` is
  off for `*/example/src/main.ts`: the `shapes` key those examples print is the output every
  language's example is compared on (`demo/expected.json`).
