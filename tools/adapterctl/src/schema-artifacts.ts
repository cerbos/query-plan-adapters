import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { schemaArtifacts } from "./schemas.ts";

const schemaDirectory = join(import.meta.dirname, "..", "schemas");

function renderSchema(schema: object): string {
  return `${JSON.stringify(schema, null, 2)}\n`;
}

async function writeSchemas(): Promise<void> {
  for (const artifact of schemaArtifacts) {
    await writeFile(join(schemaDirectory, artifact.fileName), renderSchema(artifact.schema));
  }
}

async function checkSchemas(): Promise<void> {
  const stale: string[] = [];
  for (const artifact of schemaArtifacts) {
    const actual = await readFile(join(schemaDirectory, artifact.fileName), "utf8");
    if (actual !== renderSchema(artifact.schema)) stale.push(artifact.fileName);
  }
  if (stale.length > 0) {
    throw new Error(`generated schemas are stale: ${stale.join(", ")}; run npm run schema:write`);
  }
}

const mode = process.argv[2];
if (mode === "--write") await writeSchemas();
else if (mode === "--check") await checkSchemas();
else throw new Error("usage: schema-artifacts --write|--check");
