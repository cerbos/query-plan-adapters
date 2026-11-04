// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"braces.dev/errtrace"
	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/structpb"

	sqlrewritev1 "github.com/cerbos/query-plan-adapters/sqlrewrite/generated/pb/cerbos/sqlrewrite/v1"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/generated/pb/cerbos/sqlrewrite/v1/sqlrewritev1connect"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite"
)

const mysqlTimeLayout = "2006-01-02 15:04:05.999999"

var _ sqlrewritev1connect.SQLRewriteServiceHandler = (*Service)(nil)

type Service struct {
	rewriter *rewrite.Rewriter
	log      zerolog.Logger
}

func New(rewriter *rewrite.Rewriter, log zerolog.Logger) *Service {
	return &Service{rewriter: rewriter, log: log}
}

func (s *Service) Rewrite(ctx context.Context, req *connect.Request[sqlrewritev1.RewriteRequest]) (*connect.Response[sqlrewritev1.RewriteResponse], error) {
	in := req.Msg
	params := make([]any, len(in.GetParams()))
	for i, p := range in.GetParams() {
		v, err := fromValue(p)
		if err != nil {
			return nil, refusal(&rewrite.RefusalError{Code: rewrite.CodeInvalidRequest, Message: fmt.Sprintf("params[%d]: %v", i, err)})
		}
		params[i] = v
	}

	res, err := s.rewriter.Rewrite(ctx, rewrite.Request{SQL: in.GetSql(), Params: params, Plans: in.GetPlans()})
	if err != nil {
		var re *rewrite.RefusalError
		if errors.As(err, &re) {
			s.log.Debug().Str("code", string(re.Code)).Str("reason", re.Message).Msg("Refused")
			return nil, refusal(re)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, connect.NewError(connect.CodeCanceled, ctxErr)
		}
		s.log.Error().Err(errtrace.Wrap(err)).Msg("Rewrite failed")
		return nil, connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}

	out := &sqlrewritev1.RewriteResponse{Sql: res.SQL}
	for i, p := range res.Params {
		v, err := s.toValue(p)
		if err != nil {
			s.log.Error().Err(errtrace.Wrap(err)).Int("index", i).Msg("Cannot encode param")
			return nil, connect.NewError(connect.CodeInternal, errors.New("internal error"))
		}
		out.Params = append(out.Params, v)
	}
	for _, a := range res.Applied {
		out.Applied = append(out.Applied, &sqlrewritev1.RewriteResponse_Applied{
			Resource:   a.Resource,
			Table:      a.Table,
			Kind:       planKind(a.Kind),
			References: uint32(a.References), //nolint:gosec
		})
	}
	return connect.NewResponse(out), nil
}

func refusal(re *rewrite.RefusalError) *connect.Error {
	code := connect.CodeInvalidArgument
	if re.Code == rewrite.CodeMissingPlan {
		code = connect.CodeFailedPrecondition
	}
	cerr := connect.NewError(code, re)
	if detail, err := connect.NewErrorDetail(&sqlrewritev1.RewriteRefusal{Code: refusalCode(re.Code), Message: re.Message}); err == nil {
		cerr.AddDetail(detail)
	}
	return cerr
}

func refusalCode(c rewrite.Code) sqlrewritev1.RewriteRefusal_Code {
	switch c {
	case rewrite.CodeUnsupportedPlan:
		return sqlrewritev1.RewriteRefusal_CODE_UNSUPPORTED_PLAN
	case rewrite.CodeUnsupportedSQL:
		return sqlrewritev1.RewriteRefusal_CODE_UNSUPPORTED_SQL
	case rewrite.CodeUnmappedAttribute:
		return sqlrewritev1.RewriteRefusal_CODE_UNMAPPED_ATTRIBUTE
	case rewrite.CodeMissingPlan:
		return sqlrewritev1.RewriteRefusal_CODE_MISSING_PLAN
	case rewrite.CodeUnusedPlan:
		return sqlrewritev1.RewriteRefusal_CODE_UNUSED_PLAN
	case rewrite.CodeAmbiguousReference:
		return sqlrewritev1.RewriteRefusal_CODE_AMBIGUOUS_REFERENCE
	case rewrite.CodeInvalidRequest:
		return sqlrewritev1.RewriteRefusal_CODE_INVALID_REQUEST
	default:
		return sqlrewritev1.RewriteRefusal_CODE_UNSPECIFIED
	}
}

func planKind(k rewrite.PlanKind) sqlrewritev1.PlanKind {
	switch k {
	case rewrite.KindAlwaysAllowed:
		return sqlrewritev1.PlanKind_PLAN_KIND_ALWAYS_ALLOWED
	case rewrite.KindAlwaysDenied:
		return sqlrewritev1.PlanKind_PLAN_KIND_ALWAYS_DENIED
	case rewrite.KindConditional:
		return sqlrewritev1.PlanKind_PLAN_KIND_CONDITIONAL
	default:
		return sqlrewritev1.PlanKind_PLAN_KIND_UNSPECIFIED
	}
}

func fromValue(v *structpb.Value) (any, error) {
	switch k := v.GetKind().(type) {
	case nil, *structpb.Value_NullValue:
		return nil, nil
	case *structpb.Value_BoolValue:
		return k.BoolValue, nil
	case *structpb.Value_StringValue:
		return k.StringValue, nil
	case *structpb.Value_NumberValue:
		n := k.NumberValue
		if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
			return int64(n), nil
		}
		return n, nil
	default:
		return nil, errors.New("lists and objects cannot be bound as parameters")
	}
}

func (s *Service) toValue(v any) (*structpb.Value, error) {
	if t, ok := v.(time.Time); ok {
		if s.rewriter.Dialect() == rewrite.DialectMySQL {
			return structpb.NewStringValue(t.UTC().Format(mysqlTimeLayout)), nil
		}
		return structpb.NewStringValue(t.Format(time.RFC3339Nano)), nil
	}
	return errtrace.Wrap2(structpb.NewValue(v))
}
