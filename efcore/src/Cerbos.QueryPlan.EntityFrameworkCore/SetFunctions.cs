// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using Google.Protobuf.WellKnownTypes;
using Operand = Cerbos.Api.V1.Engine.PlanResourcesFilter.Types.Expression.Types.Operand;
using PlanExpression = Cerbos.Api.V1.Engine.PlanResourcesFilter.Types.Expression;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// Rewrites hasAll, hasAny, hasOnly and hasExactly (CEL list extensions, Cerbos 0.58) into
/// hasIntersection and isSubset, bottom-up. Each rewrite is exact in CEL, errors included; a call
/// without exactly one literal list operand is left for the translator to refuse.
/// </summary>
internal static class SetFunctions
{
    public static Operand Rewrite(Operand operand)
    {
        if (operand.NodeCase != Operand.NodeOneofCase.Expression)
        {
            return operand;
        }
        var expression = operand.Expression;
        var children = expression.Operands;
        Operand? copy = null;
        for (var i = 0; i < children.Count; i++)
        {
            var child = Rewrite(children[i]);
            if (!ReferenceEquals(child, children[i]))
            {
                // The caller owns the protobuf. Copy this parent only when a child changes.
                copy ??= Call(expression.Operator, children);
                copy.Expression.Operands[i] = child;
            }
        }

        var result = copy ?? operand;
        var op = expression.Operator;
        if (op is "hasAll" or "hasAny" or "hasOnly" or "hasExactly" && children.Count == 2)
        {
            var operands = result.Expression.Operands;
            return SetFunction(op, operands[0], operands[1]) ?? result;
        }
        return result;
    }

    private static Operand? SetFunction(string op, Operand first, Operand second)
    {
        var firstLiteral = IsLiteralList(first);
        if (firstLiteral == IsLiteralList(second))
        {
            return null;
        }
        var (list, literal) = firstLiteral ? (second, first) : (first, second);
        op = (firstLiteral, op) switch
        {
            (true, "hasAll") => "hasOnly",
            (true, "hasOnly") => "hasAll",
            _ => op,
        };
        return op switch
        {
            "hasAny" => Call("hasIntersection", [list, literal]),
            "hasAll" => ContainsAll(list, literal),
            "hasOnly" => Call("isSubset", [list, literal]),
            _ => Call("and", [ContainsAll(list, literal), Call("isSubset", [list, literal])]),
        };
    }

    private static Operand ContainsAll(Operand list, Operand literal)
    {
        var values = literal.Value.ListValue.Values;
        if (values.Count == 0)
        {
            return Call("not", [Call("hasIntersection", [list, LiteralList([])])]);
        }
        var tests = values.Select(v => Call("hasIntersection", [list, LiteralList([v])])).ToList();
        return tests.Count == 1 ? tests[0] : Call("and", tests);
    }

    private static bool IsLiteralList(Operand operand) =>
        operand.NodeCase == Operand.NodeOneofCase.Value && operand.Value.KindCase == Value.KindOneofCase.ListValue;

    private static Operand LiteralList(IEnumerable<Value> values)
    {
        var list = new ListValue();
        list.Values.AddRange(values);
        return new Operand { Value = new Value { ListValue = list } };
    }

    private static Operand Call(string op, IEnumerable<Operand> operands)
    {
        var expression = new PlanExpression { Operator = op };
        expression.Operands.AddRange(operands);
        return new Operand { Expression = expression };
    }
}
