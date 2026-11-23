// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using Cerbos.Api.V1.Engine;
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
    private static readonly HashSet<string> Operators = ["hasAll", "hasAny", "hasOnly", "hasExactly"];

    public static Operand Rewrite(Operand operand)
    {
        if (operand.NodeCase != Operand.NodeOneofCase.Expression)
        {
            return operand;
        }
        var op = operand.Expression.Operator;
        var operands = operand.Expression.Operands.Select(Rewrite).ToList();
        if (Operators.Contains(op) && operands.Count == 2 && SetFunction(op, operands[0], operands[1]) is { } rewritten)
        {
            return rewritten;
        }
        return Call(op, operands);
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
