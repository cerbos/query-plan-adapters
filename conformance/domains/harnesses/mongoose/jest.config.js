const path = require("path");

const ADAPTER = path.resolve(__dirname, "../../../../mongoose");

/** @type {import("ts-jest").JestConfigWithTsJest} */
module.exports = {
  rootDir: __dirname,
  testEnvironment: "node",
  extensionsToTreatAsEsm: [".ts"],
  transform: {
    "^.+\\.[tj]sx?$": [require.resolve("ts-jest", { paths: [ADAPTER] }), { useESM: true, isolatedModules: true, tsconfig: path.join(ADAPTER, "tsconfig.json") }],
  },
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  modulePaths: [path.join(ADAPTER, "node_modules")],
  moduleNameMapper: {
    "^@cerbos/orm-mongoose$": path.join(ADAPTER, "src"),
    "^@cerbos/orm-mongoose/(.*)$": path.join(ADAPTER, "src", "$1"),
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
  testMatch: ["<rootDir>/harness.domains.ts"],
};
