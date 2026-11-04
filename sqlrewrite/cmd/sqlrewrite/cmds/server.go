// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cmds

import (
	"context"

	"braces.dev/errtrace"
	"github.com/rs/zerolog"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/config"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/server"
)

type ServerCmd struct{}

func (c *ServerCmd) Run(g GlobalArgs) error {
	return errtrace.Wrap(runHarness(g, func(ctx context.Context, log zerolog.Logger, conf *config.Provider) error {
		rewriter, err := loadRewriter(conf)
		if err != nil {
			return errtrace.Wrap(err)
		}
		var sc server.Conf
		if err := conf.GetSection(&sc); err != nil {
			return errtrace.Wrap(err)
		}
		log.Info().Str("dialect", string(rewriter.Dialect())).Msg("Rewriter ready")
		return errtrace.Wrap(server.Start(ctx, &sc, rewriter, log))
	}))
}

type CheckCmd struct{}

func (c *CheckCmd) Run(g GlobalArgs) error {
	return errtrace.Wrap(runHarness(g, func(_ context.Context, log zerolog.Logger, conf *config.Provider) error {
		rewriter, err := loadRewriter(conf)
		if err != nil {
			return errtrace.Wrap(err)
		}
		log.Info().Str("dialect", string(rewriter.Dialect())).Msg("Configuration is valid")
		return nil
	}))
}
