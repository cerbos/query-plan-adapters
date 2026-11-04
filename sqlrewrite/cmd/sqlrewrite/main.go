// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/alecthomas/kong"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/cmd/sqlrewrite/cmds"
)

var version = "dev"

type args struct {
	Server cmds.ServerCmd `cmd:"" help:"Serve the rewrite API over gRPC and HTTP"`
	Check  cmds.CheckCmd  `cmd:"" help:"Validate the configuration and exit"`
	cmds.GlobalArgs
}

func main() {
	var a args
	ctx := kong.Parse(&a,
		kong.Name("sqlrewrite"),
		kong.Description("Applies Cerbos query plans to SQL statements"),
		kong.UsageOnError(),
		kong.Vars{"version": version},
		kong.Bind(a.GlobalArgs),
	)
	ctx.FatalIfErrorf(ctx.Run(a.GlobalArgs))
}
