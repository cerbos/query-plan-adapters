import 'dotenv/config'
import { defineConfig } from 'prisma/config'

// The adversarial harness is the only suite that reaches a Prisma client, so its SQLite schema is
// the default here. Every invocation that matters passes `--schema` and `--url` explicitly (see
// package.json and jest.globalSetup.adversarial.js); this only keeps a bare `prisma` command
// pointing at a schema that exists.
export default defineConfig({
  schema: 'prisma/schema.adversarial.prisma',
  migrations: {
    path: 'prisma/migrations',
  },
  datasource: {
    url: "file:./prisma/dev-adversarial.db",
  },
})
