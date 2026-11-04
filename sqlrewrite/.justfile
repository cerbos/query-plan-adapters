set dotenv-load := true

export CGO_ENABLED := "0"

tools_mod_dir := join(justfile_directory(), "tools")

default:
    @just --list

# Regenerates generated/pb from protos/.
generate:
    buf format -w protos
    buf lint
    cd {{ tools_mod_dir }} && buf generate --template buf.gen.yaml -o ..

# Unit suites; no store.
test PKG='./...' TEST='.*':
    go test -count=1 -run='{{ TEST }}' {{ PKG }}

# The corpus replay and the SQL-shape suite on SQLite, PostgreSQL and MySQL.
test-conformance:
    go test -tags tests -count=1 -timeout 30m ./pkg/rewrite/

lint:
    golangci-lint run --config=.golangci.yaml ./...
    golangci-lint fmt --config=.golangci.yaml ./...
    buf lint

build:
    go build -o dist/sqlrewrite ./cmd/sqlrewrite
