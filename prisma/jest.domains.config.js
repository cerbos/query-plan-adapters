/** @type {import("ts-jest").JestConfigWithTsJest} */
module.exports = {
  testEnvironment: "node",
  transform: {
    "^.+\\.[tj]sx?$": ["ts-jest", { isolatedModules: true, tsconfig: { module: "commonjs", esModuleInterop: true, allowJs: true } }],
  },
  transformIgnorePatterns: ["node_modules/(?!(uuid|@cerbos)/)"],
  moduleNameMapper: { "^(\\.{1,2}/.*)\\.js$": "$1" },
  testMatch: ["<rootDir>/domains/domains.test.ts"],
  testTimeout: 600000,
  forceExit: true,
};
