const path = require("path");

const ADAPTER = path.resolve(__dirname, "../../../../drizzle");

/** @type {import("ts-jest").JestConfigWithTsJest} */
module.exports = {
  rootDir: __dirname,
  testEnvironment: "node",
  transform: {
    "^.+\\.[tj]sx?$": [
      require.resolve("ts-jest", { paths: [ADAPTER] }),
      { isolatedModules: true, tsconfig: { module: "commonjs", esModuleInterop: true, allowJs: true } },
    ],
  },
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  modulePaths: [path.join(ADAPTER, "node_modules")],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
  testMatch: ["<rootDir>/domains.test.ts"],
  testTimeout: 600000,
  forceExit: true,
};
