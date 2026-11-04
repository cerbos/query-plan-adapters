// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"net"
	"net/http"

	"braces.dev/errtrace"
	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/validate"
	"github.com/rs/zerolog"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/generated/pb/cerbos/sqlrewrite/v1/sqlrewritev1connect"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/service"
)

func Handler(conf *Conf, rewriter *rewrite.Rewriter, log zerolog.Logger) http.Handler {
	mux := http.NewServeMux()
	opts := []connect.HandlerOption{
		connect.WithInterceptors(validate.NewInterceptor()),
		connect.WithReadMaxBytes(int(conf.MaxRequestBytes)),
	}
	mux.Handle(sqlrewritev1connect.NewSQLRewriteServiceHandler(service.New(rewriter, log), opts...))
	mux.Handle(grpchealth.NewHandler(grpchealth.NewStaticChecker(sqlrewritev1connect.SQLRewriteServiceName)))
	reflector := grpcreflect.NewStaticReflector(sqlrewritev1connect.SQLRewriteServiceName)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))
	return mux
}

func Start(ctx context.Context, conf *Conf, rewriter *rewrite.Rewriter, log zerolog.Logger) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", conf.ListenAddr)
	if err != nil {
		return errtrace.Errorf("failed to listen on %s: %w", conf.ListenAddr, err)
	}
	return errtrace.Wrap(Serve(ctx, lis, conf, rewriter, log))
}

func Serve(ctx context.Context, lis net.Listener, conf *Conf, rewriter *rewrite.Rewriter, log zerolog.Logger) error {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           Handler(conf, rewriter, log),
		Protocols:         protocols,
		ReadHeaderTimeout: conf.ReadTimeout,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	errs := make(chan error, 1)
	go func() {
		log.Info().Str("addr", lis.Addr().String()).Msg("Serving")
		errs <- srv.Serve(lis)
	}()

	select {
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errtrace.Wrap(err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), conf.ShutdownTimeout)
	defer cancel()
	log.Info().Msg("Shutting down")
	return errtrace.Wrap(srv.Shutdown(shutdownCtx))
}
