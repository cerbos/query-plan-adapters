#!/usr/bin/env node
// A Prisma generator that writes the datamodel `queryPlanToPrisma` reads as its `datamodel` option.
// Prisma 7's `prisma-client` generator ships no `Prisma.dmmf`, so this is how its users get one.

const fs = require("node:fs");
const path = require("node:path");
const { generatorHandler } = require("@prisma/generator-helper");

function prune(datamodel) {
  return {
    models: datamodel.models.map((model) => ({
      name: model.name,
      fields: model.fields.map(({ name, kind, type, isList, isRequired }) => ({
        name,
        kind,
        type,
        isList,
        isRequired,
      })),
    })),
  };
}

generatorHandler({
  onManifest() {
    return { prettyName: "Cerbos query plan datamodel", defaultOutput: "./generated/cerbos" };
  },
  async onGenerate(options) {
    const output = options.generator.output?.value;

    if (!output) throw new Error("The cerbos generator needs an `output` directory");

    const json = JSON.stringify(prune(options.dmmf.datamodel), null, 2);

    fs.mkdirSync(output, { recursive: true });
    fs.writeFileSync(path.join(output, "datamodel.json"), json + "\n");
    fs.writeFileSync(
      path.join(output, "datamodel.ts"),
      `export const datamodel = ${json} as const;\n`
    );
  },
});
