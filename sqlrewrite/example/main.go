// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Example application for the Cerbos SQL rewrite service, run against the shared demo domain in
// demo/. run.sh starts the service with config.yaml; this program plans with the PDP, sends its own
// statements and the plan to the service over HTTP, and executes what comes back. It never builds
// a filter itself and never switches on the plan kind.
//
// Prints one JSON document to stdout; demo/scripts/run-example.sh diffs it against
// demo/expected.json.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"

	"connectrpc.com/connect"
	"github.com/cerbos/cerbos-sdk-go/cerbos"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/structpb"

	sqlrewritev1 "github.com/cerbos/query-plan-adapters/sqlrewrite/generated/pb/cerbos/sqlrewrite/v1"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/generated/pb/cerbos/sqlrewrite/v1/sqlrewritev1connect"
)

const (
	adapterName  = "sqlrewrite"
	resourceKind = "document"
	seedsPath    = "../../demo/seeds.json"

	// The service run.sh starts from config.yaml, and the store it starts.
	serviceURL = "http://127.0.0.1:13594"
	//nolint:gosec // G101: the credentials of a throwaway container this repository starts itself.
	dsn = "postgres://cerbos_demo:cerbos_demo@127.0.0.1:15433/cerbos_demo?sslmode=disable"

	viewPageSize      = 2
	adminViewPageSize = 3
)

const schemaDDL = `
DROP TABLE IF EXISTS document;
CREATE TABLE document (
	id         text    PRIMARY KEY,
	owner_id   text    NOT NULL,
	is_public  boolean NOT NULL,
	region     text    NOT NULL,
	archived   boolean NOT NULL
)`

type seedPrincipal struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles"`
}

type seedApplicationFilter struct {
	Description string `json:"description"`
	Region      string `json:"region"`
	Archived    bool   `json:"archived"`
}

type seedDocument struct {
	ID string `json:"id"`
	//nolint:tagliatelle // The corpus spells it ownerId.
	OwnerID  string `json:"ownerId"`
	Region   string `json:"region"`
	Public   bool   `json:"public"`
	Archived bool   `json:"archived"`
}

type seeds struct {
	Schema            string                `json:"$schema"` //nolint:tagliatelle // JSON Schema's reserved key.
	Description       string                `json:"description"`
	Principals        []seedPrincipal       `json:"principals"`
	Documents         []seedDocument        `json:"documents"`
	ApplicationFilter seedApplicationFilter `json:"applicationFilter"`
}

func loadSeeds() (*seeds, error) {
	raw, err := os.ReadFile(seedsPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", seedsPath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var parsed seeds
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", seedsPath, err)
	}
	return &parsed, nil
}

func (s *seeds) principal(id string) (*cerbos.Principal, error) {
	for _, p := range s.Principals {
		if p.ID == id {
			return cerbos.NewPrincipal(p.ID, p.Roles...), nil
		}
	}
	return nil, fmt.Errorf("no principal %q in %s", id, seedsPath)
}

type shapeResult struct {
	Kind      string   `json:"kind"`
	IDs       []string `json:"ids"` //nolint:tagliatelle // The corpus spells it ids.
	PageSizes []int    `json:"pageSizes,omitempty"`
	PageSize  int      `json:"pageSize,omitempty"`
}

type output struct {
	Shapes  map[string]map[string]shapeResult `json:"shapes"`
	Adapter string                            `json:"adapter"`
}

type app struct {
	cerbos  *cerbos.GRPCClient
	rewrite sqlrewritev1connect.SQLRewriteServiceClient
	pool    *pgxpool.Pool
	seeds   *seeds
}

var wanted = []struct {
	shape       string
	principalID string
	action      string
	pageSize    int
}{
	{shape: "filtered", principalID: "alice", action: "view"},
	{shape: "filtered", principalID: "bob", action: "view"},
	{shape: "alwaysAllowed", principalID: "admin", action: "admin-view"},
	{shape: "alwaysDenied", principalID: "alice", action: "publish"},
	{shape: "paginated", principalID: "alice", action: "view", pageSize: viewPageSize},
	{shape: "paginated", principalID: "admin", action: "admin-view", pageSize: adminViewPageSize},
	{shape: "composed", principalID: "alice", action: "view"},
	{shape: "composed", principalID: "bob", action: "view"},
	{shape: "composed", principalID: "admin", action: "admin-view"},
	{shape: "composed", principalID: "alice", action: "publish"},
}

func (a *app) shapes(ctx context.Context) (map[string]map[string]shapeResult, error) {
	shapes := map[string]map[string]shapeResult{}
	for _, want := range wanted {
		var (
			result shapeResult
			err    error
		)
		switch want.shape {
		case "paginated":
			result, err = a.paginated(ctx, want.principalID, want.action, want.pageSize)
		case "composed":
			result, err = a.composed(ctx, want.principalID, want.action)
		case "filtered", "alwaysAllowed", "alwaysDenied":
			result, err = a.filtered(ctx, want.principalID, want.action)
		default:
			return nil, fmt.Errorf("no exercise for shape %q", want.shape)
		}
		if err != nil {
			return nil, fmt.Errorf("%s/%s/%s: %w", want.shape, want.principalID, want.action, err)
		}
		if shapes[want.shape] == nil {
			shapes[want.shape] = map[string]shapeResult{}
		}
		shapes[want.shape][want.principalID+"/"+want.action] = result
	}
	return shapes, nil
}

// filtered is usage shapes 1, 2 and 3: the application's plain list query, whatever plan kind the
// policy answers with.
func (a *app) filtered(ctx context.Context, principalID, action string) (shapeResult, error) {
	plan, err := a.plan(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}
	ids, err := a.selectIDs(ctx, plan, "SELECT id FROM document")
	if err != nil {
		return shapeResult{}, err
	}
	return shapeResult{Kind: kindOf(plan), IDs: ids}, nil
}

// paginated is usage shape 4: the application's own LIMIT and OFFSET parameters, which the service
// numbers its filter's parameters after.
func (a *app) paginated(ctx context.Context, principalID, action string, pageSize int) (shapeResult, error) {
	plan, err := a.plan(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}
	pageSizes, ids := []int{}, []string{}
	for offset := 0; ; offset += pageSize {
		page, err := a.selectIDs(ctx, plan, "SELECT id FROM document ORDER BY id LIMIT $1 OFFSET $2", pageSize, offset)
		if err != nil {
			return shapeResult{}, fmt.Errorf("reading page at offset %d: %w", offset, err)
		}
		if len(page) == 0 {
			break
		}
		pageSizes = append(pageSizes, len(page))
		ids = append(ids, page...)
		if len(page) < pageSize {
			break
		}
	}
	slices.Sort(ids)
	return shapeResult{Kind: kindOf(plan), PageSize: pageSize, PageSizes: pageSizes, IDs: ids}, nil
}

// composed is usage shape 5: the application's own predicate stays in its own WHERE clause.
func (a *app) composed(ctx context.Context, principalID, action string) (shapeResult, error) {
	plan, err := a.plan(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}
	f := a.seeds.ApplicationFilter
	ids, err := a.selectIDs(ctx, plan, "SELECT d.id FROM document d WHERE d.archived = $1 AND d.region = $2", f.Archived, f.Region)
	if err != nil {
		return shapeResult{}, err
	}
	return shapeResult{Kind: kindOf(plan), IDs: ids}, nil
}

func (a *app) plan(ctx context.Context, principalID, action string) (*responsev1.PlanResourcesResponse, error) {
	principal, err := a.seeds.principal(principalID)
	if err != nil {
		return nil, err
	}
	plan, err := a.cerbos.PlanResources(ctx, principal, cerbos.NewResource(resourceKind, ""), action)
	if err != nil {
		return nil, fmt.Errorf("planning %s: %w", action, err)
	}
	if plan.GetFilter().GetKind() == enginev1.PlanResourcesFilter_KIND_UNSPECIFIED {
		return nil, errors.New("the PDP returned a plan with no kind: is $CERBOS_HOST a Cerbos PDP?")
	}
	return plan.PlanResourcesResponse, nil
}

func kindOf(plan *responsev1.PlanResourcesResponse) string {
	return plan.GetFilter().GetKind().String()
}

// selectIDs sends the statement and the plan to the service and runs the rewritten statement.
func (a *app) selectIDs(ctx context.Context, plan *responsev1.PlanResourcesResponse, stmt string, params ...any) ([]string, error) {
	req := &sqlrewritev1.RewriteRequest{Sql: stmt, Plans: map[string]*responsev1.PlanResourcesResponse{resourceKind: plan}}
	for _, p := range params {
		v, err := structpb.NewValue(p)
		if err != nil {
			return nil, err
		}
		req.Params = append(req.Params, v)
	}
	resp, err := a.rewrite.Rewrite(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, fmt.Errorf("rewriting %q: %w", stmt, err)
	}

	args := make([]any, len(resp.Msg.GetParams()))
	for i, v := range resp.Msg.GetParams() {
		args[i] = v.AsInterface()
		if n, ok := args[i].(float64); ok && n == float64(int64(n)) {
			args[i] = int64(n)
		}
	}
	fmt.Fprintf(os.Stderr, "    %s\n      args: %v\n", resp.Msg.GetSql(), args)

	rows, err := a.pool.Query(ctx, resp.Msg.GetSql(), args...)
	if err != nil {
		return nil, fmt.Errorf("executing the rewritten statement: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("reading the rows: %w", err)
	}
	slices.Sort(ids)
	return ids, nil
}

func (a *app) seed(ctx context.Context) error {
	if _, err := a.pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("creating the schema: %w", err)
	}
	for _, row := range a.seeds.Documents {
		if _, err := a.pool.Exec(ctx,
			"INSERT INTO document (id, owner_id, is_public, region, archived) VALUES ($1, $2, $3, $4, $5)",
			row.ID, row.OwnerID, row.Public, row.Region, row.Archived); err != nil {
			return fmt.Errorf("seeding %s: %w", row.ID, err)
		}
	}
	fmt.Fprintf(os.Stderr, "seeded %d documents\n", len(a.seeds.Documents))
	return nil
}

func run(ctx context.Context) error {
	host := os.Getenv("CERBOS_HOST")
	if host == "" {
		return errors.New("CERBOS_HOST is not set: run this example through demo/scripts/run-example.sh " + adapterName)
	}
	corpus, err := loadSeeds()
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("opening the connection pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("reaching PostgreSQL: %w", err)
	}

	cerbosClient, err := cerbos.New(host, cerbos.WithPlaintext())
	if err != nil {
		return fmt.Errorf("connecting to the PDP at %s: %w", host, err)
	}

	application := &app{
		cerbos:  cerbosClient,
		rewrite: sqlrewritev1connect.NewSQLRewriteServiceClient(http.DefaultClient, serviceURL),
		pool:    pool,
		seeds:   corpus,
	}
	if err := application.seed(ctx); err != nil {
		return err
	}
	shapes, err := application.shapes(ctx)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output{Adapter: adapterName, Shapes: shapes})
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s example: %v\n", adapterName, err)
		os.Exit(1)
	}
}
