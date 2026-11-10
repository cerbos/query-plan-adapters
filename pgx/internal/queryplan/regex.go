// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode"
)

// CEL's matches() without a SQL regex engine.
//
// CEL matches with RE2 (Go's regexp), and no store's regex dialect is RE2: PostgreSQL's ARE lets
// `$` match before a trailing newline and differs in classes and flags, MySQL's ICU engine too, and
// SQLite has no regex operator at all. So a pattern is never handed to the store. It is parsed here
// with Go's own regexp/syntax, which is the parser CEL uses, so what is valid and what a pattern
// means are exactly CEL's, and it is lowered only when what it matches can be said with the
// predicates the translator already emits exactly: equality, LIKE with an escaped prefix, suffix or
// substring over a FINITE set of literals, plus three infinite forms with an exact reading: every
// character drawn from a small set (`^[ab]+$`), a run of non-newline characters (`^.+$`), and a
// prefix and a suffix around such a run (`^a.*b$`).
//
// A pattern RE2 rejects is a CEL evaluation error, which denies under both polarities: UNKNOWN.
// Anything else is refused.

const (
	// maxRegexLiterals is the most literals one pattern may expand to before it is refused rather
	// than enumerated.
	maxRegexLiterals = 256
	// maxRegexSetSize is the most characters a set may hold for the every-character-in-a-set form.
	maxRegexSetSize = 64
	// maxRegexClassSize is the widest class that is enumerated at all.
	maxRegexClassSize = maxRegexSetSize * 4
)

type regexPlanKind uint8

const (
	// regexEquals: the string is one of literals.
	regexEquals regexPlanKind = iota
	// regexStartsWith, regexEndsWith, regexContains: some literal is a prefix, suffix or substring
	// of the string.
	regexStartsWith
	regexEndsWith
	regexContains
	// regexAllCharsIn: every character of the string is in chars, and there are at least min.
	regexAllCharsIn
	// regexNoNewline: no character is a newline, and there are at least min of them.
	regexNoNewline
	// regexPrefixSuffix: for some pair, the string starts with the prefix, ends with the suffix,
	// holds at least len(prefix) + min + len(suffix) characters and contains no newline.
	regexPrefixSuffix
)

// regexPlan is what one top-level alternative matches; the caller ORs the alternatives together.
type regexPlan struct {
	literals []string
	chars    []rune
	pairs    [][2]string
	min      int
	kind     regexPlanKind
}

type regexNodeKind uint8

const (
	regexSet regexNodeKind = iota
	regexDot
	regexGroup
	regexRepeat
	regexBegin
	regexEnd
)

// regexNode is the subset of RE2 syntax the lowering reads.
type regexNode struct {
	// sub is a repetition's operand; max is -1 when unbounded.
	sub *regexNode
	// chars is the set's members, in code point order.
	chars []rune
	// alts is a group's alternatives; a group with none matches nothing.
	alts     [][]regexNode
	min, max int
	kind     regexNodeKind
}

func unsupportedRegex(pattern, why string) error {
	return fmt.Errorf(
		"cannot translate matches(%q): %s. The translator lowers only patterns whose matches are a "+
			"finite set of literals under anchors, every character from a small set, or a prefix and "+
			"suffix around non-newline characters, because no store's regex dialect is RE2",
		pattern, why,
	)
}

// compileRE2 is what CEL calls, so its verdict on validity (the repeat budget included) is CEL's.
func compileRE2(pattern string) (*regexp.Regexp, bool) {
	re, err := regexp.Compile(pattern)
	return re, err == nil
}

// compileRegex plans a pattern. invalid reports a pattern RE2 rejects, which CEL raises on.
func compileRegex(pattern string) (plans []regexPlan, invalid bool, err error) {
	if _, ok := compileRE2(pattern); !ok {
		return nil, true, nil
	}
	// syntax.Parse with the same flags is the tree regexp.Compile compiled.
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, false, unsupportedRegex(pattern, err.Error())
	}
	var alternatives [][]regexNode
	if parsed.Op == syntax.OpAlternate {
		for _, sub := range parsed.Sub {
			sequence, err := regexSequence(pattern, sub)
			if err != nil {
				return nil, false, err
			}
			alternatives = append(alternatives, sequence)
		}
	} else {
		sequence, err := regexSequence(pattern, parsed)
		if err != nil {
			return nil, false, err
		}
		alternatives = [][]regexNode{sequence}
	}
	for _, alternative := range alternatives {
		plan, never, err := planRegexAlternative(pattern, alternative)
		if err != nil {
			return nil, false, err
		}
		if !never {
			plans = append(plans, plan)
		}
	}
	return plans, false, nil
}

// regexSequence flattens a parsed node into the sequence it matches.
func regexSequence(pattern string, re *syntax.Regexp) ([]regexNode, error) {
	switch re.Op {
	case syntax.OpConcat:
		var out []regexNode
		for _, sub := range re.Sub {
			sequence, err := regexSequence(pattern, sub)
			if err != nil {
				return nil, err
			}
			out = append(out, sequence...)
		}
		return out, nil
	case syntax.OpLiteral:
		out := make([]regexNode, 0, len(re.Rune))
		for _, r := range re.Rune {
			chars := []rune{r}
			if re.Flags&syntax.FoldCase != 0 {
				chars = foldOrbit(r)
			}
			out = append(out, regexNode{kind: regexSet, chars: chars})
		}
		return out, nil
	case syntax.OpEmptyMatch:
		return nil, nil
	default:
		node, err := regexAtom(pattern, re)
		if err != nil {
			return nil, err
		}
		return []regexNode{node}, nil
	}
}

// regexAtom reads one non-sequence node.
func regexAtom(pattern string, re *syntax.Regexp) (regexNode, error) {
	switch re.Op {
	case syntax.OpNoMatch:
		return regexNode{kind: regexGroup}, nil
	case syntax.OpCharClass:
		var size int
		for i := 0; i < len(re.Rune); i += 2 {
			size += int(re.Rune[i+1]-re.Rune[i]) + 1
		}
		if size > maxRegexClassSize {
			return regexNode{}, unsupportedRegex(pattern, "a character class is too wide to enumerate")
		}
		chars := make([]rune, 0, size)
		for i := 0; i < len(re.Rune); i += 2 {
			for r := re.Rune[i]; r <= re.Rune[i+1]; r++ {
				chars = append(chars, r)
			}
		}
		return regexNode{kind: regexSet, chars: chars}, nil
	case syntax.OpAnyCharNotNL:
		return regexNode{kind: regexDot}, nil
	case syntax.OpAnyChar:
		return regexNode{}, unsupportedRegex(pattern, "a dot that matches a newline ((?s)) is not read")
	case syntax.OpBeginText:
		return regexNode{kind: regexBegin}, nil
	case syntax.OpEndText:
		return regexNode{kind: regexEnd}, nil
	case syntax.OpBeginLine, syntax.OpEndLine:
		return regexNode{}, unsupportedRegex(pattern, "a multi-line anchor ((?m)) is not read")
	case syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		return regexNode{}, unsupportedRegex(pattern, "a word boundary is not read")
	case syntax.OpCapture, syntax.OpAlternate, syntax.OpConcat, syntax.OpLiteral, syntax.OpEmptyMatch:
		return regexGroupNode(pattern, re)
	case syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat:
		sub, err := regexGroupNode(pattern, re.Sub[0])
		if err != nil {
			return regexNode{}, err
		}
		if sub.kind == regexGroup && len(sub.alts) == 1 && len(sub.alts[0]) == 1 {
			sub = sub.alts[0][0]
		}
		if sub.kind == regexBegin || sub.kind == regexEnd {
			return regexNode{}, unsupportedRegex(pattern, "a repeated anchor is not read")
		}
		node := regexNode{kind: regexRepeat, sub: &sub, min: re.Min, max: re.Max}
		switch re.Op {
		case syntax.OpStar:
			node.min, node.max = 0, -1
		case syntax.OpPlus:
			node.min, node.max = 1, -1
		case syntax.OpQuest:
			node.min, node.max = 0, 1
		default:
		}
		return node, nil
	default:
		return regexNode{}, unsupportedRegex(pattern, fmt.Sprintf("the %s form is not read", re.Op))
	}
}

// regexGroupNode reads a node as a group of alternatives.
func regexGroupNode(pattern string, re *syntax.Regexp) (regexNode, error) {
	if re.Op == syntax.OpCapture {
		return regexGroupNode(pattern, re.Sub[0])
	}
	if re.Op == syntax.OpAlternate {
		alts := make([][]regexNode, 0, len(re.Sub))
		for _, sub := range re.Sub {
			sequence, err := regexSequence(pattern, sub)
			if err != nil {
				return regexNode{}, err
			}
			alts = append(alts, sequence)
		}
		return regexNode{kind: regexGroup, alts: alts}, nil
	}
	sequence, err := regexSequence(pattern, re)
	if err != nil {
		return regexNode{}, err
	}
	return regexNode{kind: regexGroup, alts: [][]regexNode{sequence}}, nil
}

// foldOrbit is every character RE2's (?i) treats as the same as r: Unicode simple folding, under
// which k also matches KELVIN SIGN and s matches LONG S.
func foldOrbit(r rune) []rune {
	orbit := []rune{r}
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		orbit = append(orbit, f)
	}
	return orbit
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// finiteRegexSequence is every string a sequence matches, when that is a finite set of at most
// maxRegexLiterals; ok is false otherwise.
func finiteRegexSequence(nodes []regexNode) (literals []string, ok bool) {
	literals = []string{""}
	for i := range nodes {
		next, ok := finiteRegexNode(&nodes[i])
		if !ok {
			return nil, false
		}
		product := make([]string, 0, len(literals)*len(next))
		for _, head := range literals {
			for _, tail := range next {
				product = append(product, head+tail)
				if len(product) > maxRegexLiterals {
					return nil, false
				}
			}
		}
		literals = uniqueStrings(product)
	}
	return literals, true
}

func finiteRegexNode(node *regexNode) ([]string, bool) {
	switch node.kind {
	case regexSet:
		out := make([]string, 0, len(node.chars))
		for _, r := range node.chars {
			out = append(out, string(r))
		}
		return out, true
	case regexGroup:
		var union []string
		for _, alt := range node.alts {
			literals, ok := finiteRegexSequence(alt)
			if !ok {
				return nil, false
			}
			union = append(union, literals...)
			if len(union) > maxRegexLiterals {
				return nil, false
			}
		}
		return uniqueStrings(union), true
	case regexRepeat:
		if node.max < 0 {
			return nil, false
		}
		var union []string
		for count := node.min; count <= node.max; count++ {
			copies := make([]regexNode, count)
			for i := range copies {
				copies[i] = *node.sub
			}
			literals, ok := finiteRegexSequence(copies)
			if !ok {
				return nil, false
			}
			union = append(union, literals...)
			if len(union) > maxRegexLiterals {
				return nil, false
			}
		}
		return uniqueStrings(union), true
	default:
		return nil, false
	}
}

// planRegexAlternative plans one top-level alternative, its anchors removed. An end without an
// anchor is free: a match may start (or stop) anywhere, so a repetition there needs only its
// minimum number of copies (`a+b` somewhere is exactly `ab` somewhere) and `.*` there needs none.
func planRegexAlternative(pattern string, sequence []regexNode) (plan regexPlan, never bool, err error) {
	nodes := append([]regexNode(nil), sequence...)
	anchoredStart, anchoredEnd := false, false
	for len(nodes) > 0 && nodes[0].kind == regexBegin {
		anchoredStart = true
		nodes = nodes[1:]
	}
	for len(nodes) > 0 && nodes[len(nodes)-1].kind == regexEnd {
		anchoredEnd = true
		nodes = nodes[:len(nodes)-1]
	}
	for _, node := range nodes {
		if node.kind == regexBegin || node.kind == regexEnd {
			return regexPlan{}, false, unsupportedRegex(pattern, "an anchor inside the pattern is not read")
		}
	}
	atLeast := func(node regexNode) regexNode {
		if node.kind == regexRepeat {
			node.max = node.min
		}
		return node
	}
	if !anchoredStart {
		for len(nodes) > 0 && nodes[0].kind == regexRepeat && nodes[0].min == 0 {
			nodes = nodes[1:]
		}
		if len(nodes) > 0 {
			nodes[0] = atLeast(nodes[0])
		}
	}
	if !anchoredEnd {
		for len(nodes) > 0 && nodes[len(nodes)-1].kind == regexRepeat && nodes[len(nodes)-1].min == 0 {
			nodes = nodes[:len(nodes)-1]
		}
		if len(nodes) > 0 {
			nodes[len(nodes)-1] = atLeast(nodes[len(nodes)-1])
		}
	}

	if literals, ok := finiteRegexSequence(nodes); ok {
		if len(literals) == 0 {
			return regexPlan{}, true, nil
		}
		kind := regexContains
		switch {
		case anchoredStart && anchoredEnd:
			kind = regexEquals
		case anchoredStart:
			kind = regexStartsWith
		case anchoredEnd:
			kind = regexEndsWith
		}
		return regexPlan{kind: kind, literals: literals}, false, nil
	}

	if anchoredStart && anchoredEnd {
		if plan, never, ok := planAnchoredRegex(nodes); ok {
			return plan, never, nil
		}
	}
	return regexPlan{}, false, unsupportedRegex(pattern, "what it matches is not a finite set of literals")
}

// planAnchoredRegex plans the infinite forms read under both anchors; ok is false for any other.
func planAnchoredRegex(nodes []regexNode) (plan regexPlan, never, ok bool) {
	// `^X*$`, `^X+$`, `^X{n,}$` over a set or `.`.
	if len(nodes) == 1 && nodes[0].kind == regexRepeat && nodes[0].max < 0 {
		only := nodes[0]
		if only.sub.kind == regexDot {
			return regexPlan{kind: regexNoNewline, min: only.min}, false, true
		}
		if only.sub.kind == regexSet && len(only.sub.chars) <= maxRegexSetSize {
			return regexPlan{kind: regexAllCharsIn, chars: only.sub.chars, min: only.min}, false, true
		}
	}
	// `^P.*S$`: a finite prefix and suffix around one unbounded run of `.`.
	runs := make([]int, 0, 1)
	for i, node := range nodes {
		if node.kind == regexRepeat && node.max < 0 && node.sub.kind == regexDot {
			runs = append(runs, i)
		}
	}
	if len(runs) != 1 {
		return regexPlan{}, false, false
	}
	run := runs[0]
	prefixes, pok := finiteRegexSequence(nodes[:run])
	suffixes, sok := finiteRegexSequence(nodes[run+1:])
	if !pok || !sok {
		return regexPlan{}, false, false
	}
	pairs := make([][2]string, 0, len(prefixes)*len(suffixes))
	for _, prefix := range prefixes {
		for _, suffix := range suffixes {
			pairs = append(pairs, [2]string{prefix, suffix})
		}
	}
	if len(pairs) == 0 {
		return regexPlan{}, true, true
	}
	if len(pairs) > maxRegexLiterals {
		return regexPlan{}, false, false
	}
	for _, pair := range pairs {
		if strings.Contains(pair[0]+pair[1], "\n") {
			return regexPlan{}, false, false
		}
	}
	return regexPlan{kind: regexPrefixSuffix, pairs: pairs, min: nodes[run].min}, false, true
}

// regexPredicate lowers the plans over a non-NULL string receiver.
func regexPredicate(receiver Expr, plans []regexPlan) (Expr, error) {
	charLength := Call{Name: FuncCharLength, Args: []Expr{receiver}}
	atLeast := func(n int) Expr {
		return Cmp{Op: OpGe, L: charLength, R: Lit{V: float64(n)}}
	}
	match := func(literal string, prefix, suffix bool) (Expr, error) {
		return likeMatch(receiver, literal, prefix, suffix)
	}
	anyOf := func(literals []string, prefix, suffix bool) (Expr, error) {
		conditions := make([]Expr, 0, len(literals))
		for _, literal := range literals {
			condition, err := match(literal, prefix, suffix)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, condition)
		}
		return or(conditions...), nil
	}
	noNewline := func() (Expr, error) {
		contains, err := match("\n", true, true)
		if err != nil {
			return nil, err
		}
		return Not{X: contains}, nil
	}

	conditions := make([]Expr, 0, len(plans))
	for _, plan := range plans {
		var condition Expr
		var err error
		switch plan.kind {
		case regexEquals:
			members := make([]Expr, 0, len(plan.literals))
			for _, literal := range plan.literals {
				members = append(members, Lit{V: literal})
			}
			condition = InList{X: receiver, Vs: members}
		case regexStartsWith:
			condition, err = anyOf(plan.literals, false, true)
		case regexEndsWith:
			condition, err = anyOf(plan.literals, true, false)
		case regexContains:
			condition, err = anyOf(plan.literals, true, true)
		case regexAllCharsIn:
			// Remove every allowed character; nothing may be left. REPLACE is byte-exact on every
			// dialect, as contains() already relies on.
			rest := receiver
			for _, r := range plan.chars {
				rest = Call{Name: FuncReplace, Args: []Expr{rest, Lit{V: string(r)}, Lit{V: ""}}}
			}
			empty := Cmp{Op: OpEq, L: Call{Name: FuncCharLength, Args: []Expr{rest}}, R: Lit{V: float64(0)}}
			condition = and(empty, atLeast(plan.min))
		case regexNoNewline:
			condition, err = noNewline()
			if err == nil {
				condition = and(condition, atLeast(plan.min))
			}
		case regexPrefixSuffix:
			pairs := make([]Expr, 0, len(plan.pairs))
			for _, pair := range plan.pairs {
				startsWith, err := match(pair[0], false, true)
				if err != nil {
					return nil, err
				}
				endsWith, err := match(pair[1], true, false)
				if err != nil {
					return nil, err
				}
				long := atLeast(len([]rune(pair[0])) + plan.min + len([]rune(pair[1])))
				pairs = append(pairs, and(startsWith, endsWith, long))
			}
			condition, err = noNewline()
			if err == nil {
				condition = and(or(pairs...), condition)
			}
		}
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
	}
	if len(conditions) == 0 {
		return BoolConst{V: false}, nil
	}
	return or(conditions...), nil
}

// matches lowers `receiver.matches(pattern)`.
//
// A NULL receiver is a missing attribute (or a null value, which has no matches()): CEL raises, so
// the whole predicate is UNKNOWN for it, even where a pattern matches every string.
func (b *builder) matches(n *node, m Mapper, negated bool) (Expr, error) {
	if len(n.operands) != binaryOperands {
		return nil, fmt.Errorf("'matches' requires exactly two operands")
	}
	receiver, err := b.value(n.operands[0], m)
	if err != nil {
		return nil, err
	}
	patternValue, err := b.value(n.operands[1], m)
	if err != nil {
		return nil, err
	}
	pattern, ok := patternValue.(string)
	if !ok {
		return nil, fmt.Errorf("matches() translates only a constant pattern: a pattern read from a row cannot be checked against RE2")
	}
	if receiver == nil || knownNonString(receiver) {
		// matches() over a null or a non-string is a CEL no-overload error: UNKNOWN.
		return Lit{V: nil}, nil
	}
	if s, ok := receiver.(string); ok {
		re, ok := compileRE2(pattern)
		if !ok {
			return Lit{V: nil}, nil
		}
		return negate(BoolConst{V: re.MatchString(s)}, negated), nil
	}
	recvExpr, err := asExpr(receiver)
	if err != nil {
		return nil, err
	}
	plans, invalid, err := compileRegex(pattern)
	if err != nil {
		return nil, err
	}
	if invalid {
		return Lit{V: nil}, nil
	}
	body, err := regexPredicate(recvExpr, plans)
	if err != nil {
		return nil, err
	}
	out := Case{Whens: []When{{Cond: IsNull{X: recvExpr, Negate: true}, Then: body}}}
	return negate(out, negated), nil
}
