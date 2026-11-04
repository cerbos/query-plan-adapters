// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cmds

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"braces.dev/errtrace"
	"github.com/alecthomas/kong"
	"github.com/rs/zerolog"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/config"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/logging"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite"
)

type GlobalArgs struct {
	Logging logging.Flags `embed:"" prefix:"log."`
	Config  config.Flags  `embed:"" prefix:"conf."`
	Version kong.VersionFlag
}

func runHarness(g GlobalArgs, run func(context.Context, zerolog.Logger, *config.Provider) error) (outErr error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New(g.Logging)
	defer func() {
		if outErr != nil {
			log.Error().Err(outErr).Msg("Exiting due to error")
		}
	}()

	conf, err := config.Load(g.Config)
	if err != nil {
		return errtrace.Wrap(err)
	}
	return errtrace.Wrap(run(ctx, log, conf))
}

func loadRewriter(conf *config.Provider) (*rewrite.Rewriter, error) {
	var rc rewrite.Config
	if err := conf.GetSection(&rc); err != nil {
		return nil, errtrace.Wrap(err)
	}
	r, err := rewrite.New(rc)
	if err != nil {
		return nil, errtrace.Errorf("invalid rewrite config: %w", err)
	}
	return r, nil
}
