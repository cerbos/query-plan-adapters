import { resolve } from "node:path";

import { runCli } from "./cli.ts";

const root = process.env["ADAPTERCTL_ROOT"] ?? resolve(import.meta.dirname, "../../..");
process.exitCode = await runCli({
  arguments_: process.argv.slice(2),
  dependencies: {
    root,
    stdout: process.stdout,
    stderr: process.stderr,
  },
});
