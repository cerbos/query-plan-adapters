import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, "..", "..", "..", "..", "prisma");
const major = process.argv[2] ?? "7";
const stores = (process.argv[3] ?? "sqlite,postgres,mysql").split(",");
const only = process.argv[4]?.split(",");
const PROVIDERS = { sqlite: "sqlite", postgres: "postgresql", mysql: "mysql" };
const domains = fs.readdirSync(path.join(here, "schemas")).map((f) => f.replace(/\.prisma$/, "")).filter((d) => !only || only.includes(d));

function header(store, dir) {
  const generator = path.join(root, "generator", "index.cjs");

  if (major === "7") {
    return `generator client {\n  provider     = "prisma-client"\n  output       = "./client"\n  engineType   = "client"\n  moduleFormat = "cjs"\n}\n\ngenerator cerbos {\n  provider = "node ${generator}"\n  output   = "./cerbos"\n}\n\ndatasource db {\n  provider = "${PROVIDERS[store]}"\n}\n`;
  }

  const url = store === "sqlite" ? `"file:${path.join(dir, "domain.db")}?connection_limit=1"` : `env("DATABASE_URL")`;

  return `generator client {\n  provider = "prisma-client-js"\n  output   = "./client"\n}\n\ndatasource db {\n  provider = "${PROVIDERS[store]}"\n  url      = ${url}\n}\n`;
}

function models(domain, store) {
  return fs.readFileSync(path.join(here, "schemas", `${domain}.prisma`), "utf8")
    .split("\n")
    .filter((line) => {
      const tags = line.match(/\/\/ (@\w+(?: @\w+)*)$/)?.[1];

      return !tags || tags.split(" ").includes(`@${store}`);
    })
    .map((line) => line.replace(/\s*\/\/ @\w+(?: @\w+)*$/, ""))
    .join("\n");
}

const cli = major === "7" ? "node_modules/prisma/build/index.js" : "node_modules/prisma-v6/build/index.js";
const clients = path.join(root, "node_modules", "@prisma");

if (major === "6") {
  fs.renameSync(path.join(clients, "client"), path.join(clients, "client-v7-temp"));
  fs.renameSync(path.join(clients, "client-v6"), path.join(clients, "client"));
}

try {
for (const store of stores) {
  for (const domain of domains) {
    const dir = path.join(here, "generated", `v${major}`, store, domain);
    fs.mkdirSync(dir, { recursive: true });
    const schema = path.join(dir, "schema.prisma");
    fs.writeFileSync(schema, `${header(store, dir)}\n${models(domain, store)}`);
    execFileSync(process.execPath, [cli, "generate", `--schema=${schema}`], { cwd: root, stdio: ["ignore", "ignore", "inherit"] });
    console.log(`generated v${major} ${store} ${domain}`);
  }
}
} finally {
  if (major === "6") {
    fs.renameSync(path.join(clients, "client"), path.join(clients, "client-v6"));
    fs.renameSync(path.join(clients, "client-v7-temp"), path.join(clients, "client"));
  }
}
