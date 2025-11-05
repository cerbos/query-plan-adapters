# TDD Execution Guide - Strict Protocol

## Purpose

This guide enforces strict Test-Driven Development (TDD) discipline for implementing the PyPika Cerbos adapter. Every developer and agent MUST follow this protocol without exception.

## The Iron Laws of TDD

### Law 1: One Test at a Time
**NEVER write multiple tests before implementing.**

❌ **WRONG:**
```python
def test_equals(): ...
def test_not_equals(): ...
def test_less_than(): ...

# Then implement all three
```

✅ **CORRECT:**
```python
# Write ONE test
def test_equals(): ...

# Implement to make it pass
# Commit

# THEN write next test
def test_not_equals(): ...
```

### Law 2: Always See Red Before Green
**NEVER write implementation without first seeing the test fail.**

❌ **WRONG:**
```bash
# Write test and implementation together
# Run tests
source .venv/bin/activate && pytest  # Everything passes first try
```

✅ **CORRECT:**
```bash
# Write test only
source .venv/bin/activate && pytest tests/test_query.py::test_equals  # MUST FAIL (RED)

# Write implementation
source .venv/bin/activate && pytest tests/test_query.py::test_equals  # MUST PASS (GREEN)
```

### Law 3: Commit After Every Green
**NEVER move to next test without committing current green state.**

❌ **WRONG:**
```bash
# Get test_equals passing
# Get test_not_equals passing
# Get test_less_than passing
git commit -m "Add all comparison operators"
```

✅ **CORRECT:**
```bash
# Get test_equals passing
git add .
git commit -m "Add eq operator"

# Get test_not_equals passing  
git add .
git commit -m "Add ne operator"

# And so on...
```

### Law 4: Run Single Test, Not Suite
**ALWAYS run the specific test you're working on, not the entire suite.**

❌ **WRONG:**
```bash
source .venv/bin/activate && pytest  # Runs all tests, slow feedback
```

✅ **CORRECT:**
```bash
source .venv/bin/activate && pytest tests/test_query.py::test_equals -v  # Fast, focused feedback
```

### Law 5: Minimal Implementation Only
**Write ONLY enough code to make current test pass.**

❌ **WRONG:**
```python
# Test only checks eq operator
def test_equals():
    # ... test eq ...

# But implementation adds eq, ne, lt, gt all at once
OPERATOR_FNS = {
    "eq": lambda f, v: f == v,
    "ne": lambda f, v: f != v,  # NOT NEEDED YET
    "lt": lambda f, v: f < v,   # NOT NEEDED YET
    "gt": lambda f, v: f > v,   # NOT NEEDED YET
}
```

✅ **CORRECT:**
```python
# Test only checks eq operator
def test_equals():
    # ... test eq ...

# Implementation ONLY adds eq
OPERATOR_FNS = {
    "eq": lambda f, v: f == v,
}
```

## The RED-GREEN-REFACTOR Cycle

Every feature follows this exact cycle:

```
┌─────────────────────────────────────┐
│ 1. RED: Write failing test         │
│    - Write ONE test                 │
│    - Run test (MUST see failure)    │
│    - Confirm failure message        │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│ 2. GREEN: Make test pass            │
│    - Write MINIMAL implementation   │
│    - Run test (MUST see success)    │
│    - All tests must still pass      │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│ 3. COMMIT: Save green state         │
│    - git add .                      │
│    - git commit -m "Add X"          │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│ 4. REFACTOR: Improve code (optional)│
│    - Improve code quality           │
│    - Keep tests green               │
│    - Run tests after each change    │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│ 5. COMMIT: Save refactor (if any)   │
│    - git add .                      │
│    - git commit -m "Refactor X"     │
└─────────┬───────────────────────────┘
          │
          ▼
       REPEAT for next test
```

## Phase-by-Phase TDD Protocol

### Phase 0: Project Setup (No TDD Yet)

Standard project scaffolding - no tests needed.

```bash
# Create structure
mkdir -p pypika/src/cerbos_pypika pypika/tests

# Verify structure
ls -la pypika/

# No commit until structure is complete
```

---

### Phase 0.5: Test Infrastructure (NEW)

**Goal**: Set up test fixtures BEFORE writing any feature tests

**Time**: 30 minutes

#### Step 1: Create conftest.py

```python
# tests/conftest.py
import pytest
from pypika import Table
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)

class Resource(Base):
    __tablename__ = "resource"
    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    aBool = Column(Boolean)
    aString = Column(String)
    aNumber = Column(Integer)

@pytest.fixture(scope="module")
def engine():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    # ... populate test data
    yield engine

@pytest.fixture
def resource_table():
    return Table('resource')

@pytest.fixture
def user_table():
    return Table('user')
```

#### Step 2: Verify fixtures work

```bash
# Create empty test file
touch tests/test_query.py

# Add simple test to verify fixtures
cat > tests/test_query.py << 'EOF'
def test_fixtures_work(resource_table):
    assert resource_table is not None
    assert resource_table.get_table_name() == 'resource'
EOF

# Run test
source .venv/bin/activate && pytest tests/test_query.py::test_fixtures_work -v

# Should PASS
```

#### Step 3: Commit

```bash
git add tests/conftest.py tests/test_query.py
git commit -m "Set up test fixtures"
```

---

### Phase 1: Core Infrastructure (No TDD Yet)

Types and constants don't need tests - they're definitions.

---

### Phase 1.5: Document-Driven Design (NEW)

**Goal**: Write README examples BEFORE implementing features

**Time**: 30 minutes

#### Step 1: Write desired API in README.md

```markdown
# Usage

```python
from pypika import Table
from cerbos_pypika import get_query

resources = Table('resources')

# Simple query
plan = cerbos_client.plan_resources("view", principal, resource_desc)
attr_map = {
    "request.resource.attr.status": resources.status,
}

query = get_query(plan, resources, attr_map)
sql = query.get_sql()
```
```

#### Step 2: Commit

```bash
git add README.md
git commit -m "Document desired API"
```

This README becomes your acceptance criteria!

---

### Phase 2.1: ALWAYS_ALLOWED/DENIED (TDD)

**Time**: 30 minutes (15 min per test)

#### Test 1: ALWAYS_ALLOWED

**RED:**
```bash
# Write test ONLY
cat >> tests/test_query.py << 'EOF'

def test_always_allow(resource_table):
    # Synthetic plan (no Cerbos needed yet)
    from cerbos.sdk.model import PlanResourcesFilter, PlanResourcesFilterKind, PlanResourcesResponse
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.ALWAYS_ALLOWED,
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    query = get_query(plan, resource_table, {})
    sql = query.get_sql()
    
    assert "WHERE" not in sql
EOF

# Run test - MUST FAIL
source .venv/bin/activate && pytest tests/test_query.py::test_always_allow -v

# Verify you see failure (probably ImportError or NotImplementedError)
```

**GREEN:**
```python
# Implement ONLY enough to pass this test
def get_query(query_plan, table, attr_map, joins=None, operator_override_fns=None):
    from pypika import Query
    
    if query_plan.filter.kind in _allow_types:
        return Query.from_(table).select('*')
    
    raise NotImplementedError("Not yet implemented")
```

```bash
# Run test - MUST PASS
source .venv/bin/activate && pytest tests/test_query.py::test_always_allow -v
```

**COMMIT:**
```bash
git add src/cerbos_pypika/query.py tests/test_query.py
git commit -m "Implement ALWAYS_ALLOWED handling"
```

#### Test 2: ALWAYS_DENIED

**RED:**
```bash
# Write test ONLY
cat >> tests/test_query.py << 'EOF'

def test_always_deny(resource_table):
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.ALWAYS_DENIED,
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    query = get_query(plan, resource_table, {})
    sql = query.get_sql()
    
    # PyPika represents False in WHERE clause
    assert "WHERE" in sql
EOF

# Run test - MUST FAIL
source .venv/bin/activate && pytest tests/test_query.py::test_always_deny -v
```

**GREEN:**
```python
# Extend get_query
def get_query(query_plan, table, attr_map, joins=None, operator_override_fns=None):
    from pypika import Query
    
    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        return Query.from_(table).select('*').where(False)
    
    if query_plan.filter.kind in _allow_types:
        return Query.from_(table).select('*')
    
    raise NotImplementedError("Conditional filtering not yet implemented")
```

```bash
# Run test - MUST PASS
source .venv/bin/activate && pytest tests/test_query.py::test_always_deny -v

# Run both tests
source .venv/bin/activate && pytest tests/test_query.py::test_always_allow tests/test_query.py::test_always_deny -v
```

**COMMIT:**
```bash
git add src/cerbos_pypika/query.py tests/test_query.py
git commit -m "Implement ALWAYS_DENIED handling"
```

---

### Phase 2.2: Comparison Operators (STRICT TDD)

**Total Time**: 2 hours (20 min per operator)

Each operator follows EXACT same pattern:
1. RED: Write one test
2. GREEN: Make it pass
3. COMMIT: Save progress
4. REPEAT

#### Operator 1: `eq` (30 minutes)

This is the MOST IMPORTANT operator because it drives the AST traversal implementation.

**RED:**
```python
# tests/test_query.py

def test_eq_operator(resource_table):
    """Test equality operator."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
                ],
            },
        },
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    attr_map = {
        "request.resource.attr.name": resource_table.name,
    }
    
    query = get_query(plan, resource_table, attr_map)
    sql = query.get_sql()
    
    assert "WHERE" in sql
    assert "name" in sql
```

```bash
# Run test - MUST FAIL
source .venv/bin/activate && pytest tests/test_query.py::test_eq_operator -v

# Should see NotImplementedError
```

**GREEN:**
```python
# src/cerbos_pypika/query.py

# Add to operator map
OPERATOR_FNS = MappingProxyType({
    "eq": lambda field, value: field == value,
})

# Implement traversal (MINIMAL)
def traverse_and_map_operands(operand, attr_map):
    if exp := operand.get("expression"):
        return traverse_and_map_operands(exp, attr_map)
    
    operator = operand["operator"]
    child_operands = operand["operands"]
    
    # Extract variable and value
    d = {k: v for o in child_operands for k, v in o.items()}
    variable = d["variable"]
    value = d["value"]
    
    # Get field
    field = attr_map[variable]
    
    # Apply operator
    operator_fn = OPERATOR_FNS[operator]
    return operator_fn(field, value)

# Update get_query to use traversal
def get_query(query_plan, table, attr_map, joins=None, operator_override_fns=None):
    # ... ALWAYS_ALLOWED/DENIED handling ...
    
    # Handle conditional
    cond_dict = query_plan.filter.condition.to_dict()
    criterion = traverse_and_map_operands(cond_dict, attr_map)
    
    query = Query.from_(table).select('*').where(criterion)
    return query
```

```bash
# Run test - MUST PASS
source .venv/bin/activate && pytest tests/test_query.py::test_eq_operator -v

# Run all tests to ensure no regression
source .venv/bin/activate && pytest tests/test_query.py -v
```

**COMMIT:**
```bash
git add src/cerbos_pypika/query.py tests/test_query.py
git commit -m "Implement eq operator with AST traversal"
```

#### Operator 2: `ne` (15 minutes)

**RED:**
```python
def test_ne_operator(resource_table):
    """Test not-equal operator."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "ne",
                "operands": [
                    {"variable": "request.resource.attr.name"},
                    {"value": "test"},
                ],
            },
        },
    })
    # ... rest of test setup ...
    
    sql = query.get_sql()
    assert "!=" in sql or "<>" in sql
```

```bash
source .venv/bin/activate && pytest tests/test_query.py::test_ne_operator -v  # MUST FAIL
```

**GREEN:**
```python
# Add to OPERATOR_FNS
OPERATOR_FNS = MappingProxyType({
    "eq": lambda field, value: field == value,
    "ne": lambda field, value: field != value,  # ADD THIS ONLY
})
```

```bash
source .venv/bin/activate && pytest tests/test_query.py::test_ne_operator -v  # MUST PASS
source .venv/bin/activate && pytest tests/test_query.py -v  # All tests still pass
```

**COMMIT:**
```bash
git add src/cerbos_pypika/query.py tests/test_query.py
git commit -m "Add ne operator"
```

#### Operator 3: `lt` (15 minutes)

Follow exact same pattern:
1. Write test_lt_operator
2. Run (RED)
3. Add "lt": lambda to OPERATOR_FNS
4. Run (GREEN)
5. Commit

#### Operator 4: `gt` (15 minutes)

Same pattern.

#### Operator 5: `le` (15 minutes)

Same pattern.

#### Operator 6: `ge` (15 minutes)

Same pattern.

#### Refactor (15 minutes)

After all 6 operators pass:

```bash
# All tests green?
source .venv/bin/activate && pytest tests/test_query.py -v

# Now refactor get_operator_fn
# Extract error handling
# Improve naming
# Keep tests green after each change

git add .
git commit -m "Refactor operator handling"
```

---

### Phase 2.3: Logical Operators (STRICT TDD)

**Time**: 1 hour (20 min per operator)

#### Operator: `and` (20 minutes)

**RED:**
```python
def test_and_operator(resource_table):
    """Test AND logical operator."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": "and",
                "operands": [
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.name"},
                                {"value": "test"},
                            ],
                        }
                    },
                    {
                        "expression": {
                            "operator": "eq",
                            "operands": [
                                {"variable": "request.resource.attr.status"},
                                {"value": "active"},
                            ],
                        }
                    },
                ],
            },
        },
    })
    # ... rest ...
    
    sql = query.get_sql()
    assert "AND" in sql
```

```bash
source .venv/bin/activate && pytest tests/test_query.py::test_and_operator -v  # MUST FAIL
```

**GREEN:**
```python
# Extend traverse_and_map_operands
def traverse_and_map_operands(operand, attr_map):
    if exp := operand.get("expression"):
        return traverse_and_map_operands(exp, attr_map)
    
    operator = operand["operator"]
    child_operands = operand["operands"]
    
    # NEW: Handle AND
    if operator == "and":
        criteria = [traverse_and_map_operands(o, attr_map) for o in child_operands]
        result = criteria[0]
        for c in criteria[1:]:
            result = result & c
        return result
    
    # Existing: Handle comparison operators
    # ...
```

```bash
source .venv/bin/activate && pytest tests/test_query.py::test_and_operator -v  # MUST PASS
```

**COMMIT:**
```bash
git add .
git commit -m "Add AND logical operator"
```

#### Operator: `or` (20 minutes)

Same pattern as AND.

#### Operator: `not` (20 minutes)

Same pattern.

---

## Agent-Specific Instructions

### For General-Purpose Agents

When assigned a task like "Implement comparison operators":

**YOU MUST:**

1. Read this guide fully before starting
2. Implement ONE operator at a time
3. Show RED-GREEN cycle for EACH operator
4. Commit after EACH green test
5. Report progress after each commit

**YOU MUST NOT:**

1. Write all 6 tests before implementing any
2. Implement multiple operators in one commit
3. Skip showing RED state
4. Batch commits together

### Example Agent Execution

```
Agent: Starting Milestone 2.2 - Comparison Operators

Step 1/6: Implementing eq operator
- Writing test_eq_operator... DONE
- Running test: source .venv/bin/activate && pytest tests/test_query.py::test_eq_operator -v
- Status: FAILED (RED) ✓ 
- Implementing minimal code...
- Running test: source .venv/bin/activate && pytest tests/test_query.py::test_eq_operator -v
- Status: PASSED (GREEN) ✓
- Committing: "Implement eq operator with AST traversal"

Step 2/6: Implementing ne operator
- Writing test_ne_operator... DONE
- Running test: source .venv/bin/activate && pytest tests/test_query.py::test_ne_operator -v
- Status: FAILED (RED) ✓
- Adding ne to OPERATOR_FNS...
- Running test: source .venv/bin/activate && pytest tests/test_query.py::test_ne_operator -v
- Status: PASSED (GREEN) ✓
- Committing: "Add ne operator"

[Continue for remaining operators...]

Milestone 2.2 Complete:
- 6 operators implemented
- 6 tests passing
- 7 commits made (6 operators + 1 refactor)
```

## Verification Checklist

Before moving to next phase, verify:

- [ ] Every test was RED before GREEN
- [ ] Every operator has its own commit
- [ ] No operator was implemented without a test
- [ ] All tests pass: `source .venv/bin/activate && pytest tests/ -v`
- [ ] Git log shows incremental progress: `git log --oneline`
- [ ] Each commit message is clear and specific

## Common Violations and Fixes

### Violation 1: Batching Tests

❌ **WRONG:**
```python
# Writing multiple tests
def test_eq(): ...
def test_ne(): ...
def test_lt(): ...

# Then implementing all at once
```

🔧 **FIX:**
```bash
# Delete test_ne and test_lt
# Implement only test_eq
# Get to green
# Commit
# THEN write test_ne
```

### Violation 2: Not Seeing Red

❌ **WRONG:**
```bash
# Write test and implementation together
source .venv/bin/activate && pytest  # Everything passes (no RED phase)
```

🔧 **FIX:**
```bash
# Delete implementation
# Run test (should FAIL)
source .venv/bin/activate && pytest tests/test_query.py::test_eq_operator -v
# Re-implement
# Run test (should PASS)
source .venv/bin/activate && pytest tests/test_query.py::test_eq_operator -v
```

### Violation 3: No Commits

❌ **WRONG:**
```bash
# Implement 6 operators
# One big commit at end
```

🔧 **FIX:**
```bash
# git log to see current state
# git rebase -i to split commits (advanced)
# OR: Start over with proper TDD discipline
```

## Summary

**Remember:**
- ONE test at a time
- ALWAYS see RED before GREEN
- COMMIT after every GREEN
- RUN specific test, not suite
- MINIMAL implementation only

**This is not optional. This is how TDD works.**

Following this guide ensures:
- ✅ Incremental progress
- ✅ Always shippable code
- ✅ Clear git history
- ✅ No "big bang" integration
- ✅ Fast feedback loops
- ✅ Confidence in every change

**Good luck! Follow the protocol and you'll build high-quality, well-tested code.**
