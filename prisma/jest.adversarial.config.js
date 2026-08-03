const prismaVersion = process.env.PRISMA_VERSION || "7";

/** @type {import("ts-jest").JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest/presets/default-esm",
  testEnvironment: "node",
  extensionsToTreatAsEsm: [".ts"],
  transform: {
    "^.+\\.[tj]sx?$": [
      "ts-jest",
      {
        useESM: true,
        tsconfig: "tsconfig.jest.json",
      },
    ],
  },
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
    "^(.*)/test-setup\\.adversarial$":
      `$1/test-setup.adversarial.v${prismaVersion}`,
  },
  // Isolated from the main jest.config.js run: this suite talks to a Cerbos sidecar loaded
  // with conformance/policies (resource kind "adversarial"), not ../policies.
  testMatch: ["<rootDir>/src/adversarial.test.ts"],
};
