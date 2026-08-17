import { expect, test } from "@jest/globals";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { loadActionControlPlane } from "./controlPlane";

function withControlPlaneFixture({
  outcomes,
  run,
}: {
  outcomes: Record<string, unknown>;
  run: (rootDirectory: string) => void;
}): void {
  const rootDirectory = fs.mkdtempSync(
    path.join(os.tmpdir(), "prisma-control-plane-"),
  );
  try {
    fs.mkdirSync(path.join(rootDirectory, "conformance"));
    fs.mkdirSync(path.join(rootDirectory, "prisma"));
    fs.writeFileSync(
      path.join(rootDirectory, "conformance", "catalog.json"),
      JSON.stringify({
        schemaVersion: 1,
        actions: [
          { name: "new-action", oracleExpectation: { kind: "proper-subset" } },
        ],
      }),
    );
    fs.writeFileSync(
      path.join(rootDirectory, "prisma", "adapterctl.json"),
      JSON.stringify({ schemaVersion: 1, adapter: "prisma", outcomes }),
    );
    run(rootDirectory);
  } finally {
    fs.rmSync(rootDirectory, { recursive: true, force: true });
  }
}

test("a focused unassessed action executes provisionally against the oracle", () => {
  withControlPlaneFixture({
    outcomes: { "new-action": { status: "unassessed" } },
    run: (rootDirectory) => {
      const focused = loadActionControlPlane({
        adapter: "prisma",
        selectedAction: "new-action",
        rootDirectory,
      });

      expect(focused.oracleActions).toEqual(["new-action"]);
      expect(focused.unassessedActions).toEqual([]);
    },
  });
});

test("a focused action missing from the manifest executes provisionally against the oracle", () => {
  withControlPlaneFixture({
    outcomes: {},
    run: (rootDirectory) => {
      const focused = loadActionControlPlane({
        adapter: "prisma",
        selectedAction: "new-action",
        rootDirectory,
      });

      expect(focused.oracleActions).toEqual(["new-action"]);
      expect(focused.unassessedActions).toEqual([]);
    },
  });
});

test("an unscoped run requires every catalog action to be assessed", () => {
  withControlPlaneFixture({
    outcomes: { "new-action": { status: "unassessed" } },
    run: (rootDirectory) => {
      expect(() =>
        loadActionControlPlane({
          adapter: "prisma",
          selectedAction: undefined,
          rootDirectory,
        }),
      ).toThrow(
        "prisma/adapterctl.json has an unassessed outcome for new-action",
      );
    },
  });
});

test("an unscoped run requires an outcome for every catalog action", () => {
  withControlPlaneFixture({
    outcomes: {},
    run: (rootDirectory) => {
      expect(() =>
        loadActionControlPlane({
          adapter: "prisma",
          selectedAction: undefined,
          rootDirectory,
        }),
      ).toThrow(
        "prisma/adapterctl.json.outcomes must account for every catalog action exactly once",
      );
    },
  });
});
