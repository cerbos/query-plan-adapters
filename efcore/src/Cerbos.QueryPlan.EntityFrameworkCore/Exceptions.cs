// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// The adapter refuses the plan: it holds a shape the adapter cannot translate into a predicate that
/// returns exactly the rows Cerbos allows. Deny the request, or rewrite the policy.
/// </summary>
public class QueryPlanRefusedException : Exception
{
    /// <summary>Creates a refusal.</summary>
    public QueryPlanRefusedException(string message) : base(message) { }

    /// <summary>Creates a refusal with a cause.</summary>
    public QueryPlanRefusedException(string message, Exception inner) : base(message, inner) { }

    /// <summary>Creates a refusal with no message.</summary>
    public QueryPlanRefusedException() { }
}

/// <summary>The plan reads a variable the <see cref="AttributeMap{TEntity}"/> does not map, or maps in a form the operator cannot use.</summary>
public class UnmappedAttributeException : QueryPlanRefusedException
{
    /// <summary>Creates the refusal.</summary>
    public UnmappedAttributeException(string message) : base(message) { }

    /// <summary>Creates the refusal with a cause.</summary>
    public UnmappedAttributeException(string message, Exception inner) : base(message, inner) { }

    /// <summary>Creates the refusal with no message.</summary>
    public UnmappedAttributeException() { }
}
