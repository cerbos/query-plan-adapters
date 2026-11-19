package main

import (
	"path/filepath"
	"strings"
)

type lang int

const (
	langGo lang = iota
	langTS
	langPy
	langKotlin
)

func langOf(path string) (lang, bool) {
	switch filepath.Ext(path) {
	case ".go":
		return langGo, true
	case ".ts":
		return langTS, true
	case ".py":
		return langPy, true
	case ".kt", ".java":
		return langKotlin, true
	}
	return 0, false
}

// site is one mutation: src[start:end] becomes repl.
type site struct {
	op    string
	start int
	end   int
	repl  string
}

func isIdent(b byte) bool {
	return b == '_' || b == '$' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

// codeMask marks the bytes outside comments and string, template and regex literals.
func codeMask(src string, l lang) []bool {
	mask := make([]bool, len(src))
	scanCode(src, 0, l, mask, false)
	return mask
}

// scanCode marks code from i until EOF, or until the brace closing a template
// expression when inTemplate is set, and returns the index after it.
func scanCode(src string, i int, l lang, mask []bool, inTemplate bool) int {
	depth := 0
	lastSig := byte(0)
	lastWord := ""
	for i < len(src) {
		c := src[i]
		rest := src[i:]
		switch {
		case l == langPy && c == '#':
			i = skipLine(src, i)
			continue
		case l != langPy && strings.HasPrefix(rest, "//"):
			i = skipLine(src, i)
			continue
		case l != langPy && strings.HasPrefix(rest, "/*"):
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return len(src)
			}
			i += end + 4
			continue
		case l == langPy && (c == '"' || c == '\''):
			i = skipPyString(src, i)
			lastSig, lastWord = 'x', ""
			continue
		case l == langPy && isPyPrefix(src, i):
			j := i
			for src[j] != '"' && src[j] != '\'' {
				j++
			}
			i = skipPyString(src, j)
			lastSig, lastWord = 'x', ""
			continue
		case c == '"' || c == '\'' && l != langGo && l != langKotlin:
			i = skipQuoted(src, i, c)
			lastSig, lastWord = 'x', ""
			continue
		case c == '\'':
			i = skipQuoted(src, i, c)
			lastSig, lastWord = 'x', ""
			continue
		case c == '`' && l == langGo:
			end := strings.IndexByte(src[i+1:], '`')
			if end < 0 {
				return len(src)
			}
			i += end + 2
			lastSig, lastWord = 'x', ""
			continue
		case c == '`' && l == langTS:
			i = skipTemplate(src, i, l, mask)
			lastSig, lastWord = 'x', ""
			continue
		case c == '/' && l == langTS && regexAllowed(lastSig, lastWord):
			i = skipRegex(src, i)
			lastSig, lastWord = 'x', ""
			continue
		}
		if inTemplate {
			if c == '{' {
				depth++
			} else if c == '}' {
				if depth == 0 {
					return i + 1
				}
				depth--
			}
		}
		mask[i] = true
		if isIdent(c) {
			j := i
			for j < len(src) && isIdent(src[j]) {
				mask[j] = true
				j++
			}
			lastWord = src[i:j]
			lastSig = 'x'
			i = j
			continue
		}
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			lastSig, lastWord = c, ""
		}
		i++
	}
	return i
}

func skipLine(src string, i int) int {
	end := strings.IndexByte(src[i:], '\n')
	if end < 0 {
		return len(src)
	}
	return i + end
}

func skipQuoted(src string, i int, q byte) int {
	for j := i + 1; j < len(src); j++ {
		switch src[j] {
		case '\\':
			j++
		case q:
			return j + 1
		case '\n':
			return j
		}
	}
	return len(src)
}

func skipTemplate(src string, i int, l lang, mask []bool) int {
	for j := i + 1; j < len(src); j++ {
		switch {
		case src[j] == '\\':
			j++
		case src[j] == '`':
			return j + 1
		case src[j] == '$' && j+1 < len(src) && src[j+1] == '{':
			sub := make([]bool, len(mask))
			j = scanCode(src, j+2, l, sub, true) - 1
		}
	}
	return len(src)
}

func regexAllowed(lastSig byte, lastWord string) bool {
	if lastWord != "" {
		switch lastWord {
		case "return", "typeof", "case", "do", "else", "in", "of", "new", "throw":
			return true
		}
		return false
	}
	return lastSig == 0 || strings.IndexByte("(,=:[!&|?{};+-*%<>~^", lastSig) >= 0
}

func skipRegex(src string, i int) int {
	inClass := false
	for j := i + 1; j < len(src); j++ {
		switch src[j] {
		case '\\':
			j++
		case '[':
			inClass = true
		case ']':
			inClass = false
		case '\n':
			return j
		case '/':
			if !inClass {
				j++
				for j < len(src) && isIdent(src[j]) {
					j++
				}
				return j
			}
		}
	}
	return len(src)
}

func isPyPrefix(src string, i int) bool {
	if i > 0 && isIdent(src[i-1]) {
		return false
	}
	j := i
	for j < len(src) && j-i < 2 && strings.IndexByte("rbfuRBFU", src[j]) >= 0 {
		j++
	}
	return j > i && j < len(src) && (src[j] == '"' || src[j] == '\'')
}

func skipPyString(src string, i int) int {
	q := src[i]
	if strings.HasPrefix(src[i:], strings.Repeat(string(q), 3)) {
		end := strings.Index(src[i+3:], strings.Repeat(string(q), 3))
		for end >= 0 && src[i+3+end-1] == '\\' {
			next := strings.Index(src[i+3+end+1:], strings.Repeat(string(q), 3))
			if next < 0 {
				return len(src)
			}
			end += next + 1
		}
		if end < 0 {
			return len(src)
		}
		return i + 3 + end + 3
	}
	return skipQuoted(src, i, q)
}

// sites lists every mutation of src, in source order.
func sites(src string, l lang) []site {
	mask := codeMask(src, l)
	code := func(i int) bool { return i >= 0 && i < len(mask) && mask[i] }
	at := func(i int, s string) bool {
		if !strings.HasPrefix(src[i:], s) {
			return false
		}
		for k := range len(s) {
			if !code(i + k) {
				return false
			}
		}
		return true
	}
	wordAt := func(i int, w string) bool {
		return at(i, w) && (i == 0 || !isIdent(src[i-1])) && (i+len(w) >= len(src) || !isIdent(src[i+len(w)]))
	}
	spaced := func(i, n int) bool {
		return i > 0 && i+n < len(src) && (src[i-1] == ' ' || src[i-1] == '\n') && (src[i+n] == ' ' || src[i+n] == '\n')
	}
	prevSig := func(i int) byte {
		for j := i - 1; j >= 0; j-- {
			if !code(j) {
				return 'x'
			}
			if src[j] != ' ' && src[j] != '\t' && src[j] != '\n' && src[j] != '\r' {
				return src[j]
			}
		}
		return 0
	}
	firstOnLine := func(i int) bool {
		for j := i - 1; j >= 0 && src[j] != '\n'; j-- {
			if src[j] != ' ' && src[j] != '\t' {
				return false
			}
		}
		return true
	}

	var out []site
	add := func(op string, start, end int, repl string) {
		out = append(out, site{op: op, start: start, end: end, repl: repl})
	}
	falseWord, trueWord, andWord := "false", "true", "&&"
	if l == langPy {
		falseWord, trueWord, andWord = "False", "True", "and"
	}

	for i := 0; i < len(src); i++ {
		if !code(i) {
			continue
		}
		switch {
		case l == langTS && (at(i, "===") || at(i, "!==")):
			repl := "!=="
			if src[i] == '!' {
				repl = "==="
			}
			add("equality", i, i+3, repl)
			i += 2
		case at(i, "==") && !at(i+2, "="), at(i, "!=") && !at(i+2, "="):
			repl := "!="
			if src[i] == '!' {
				repl = "=="
			}
			add("equality", i, i+2, repl)
			i++
		case l != langPy && (at(i, "&&") || at(i, "||")) && !at(i+2, "="):
			repl := "||"
			if src[i] == '|' {
				repl = "&&"
			}
			add("logical", i, i+2, repl)
			i++
		case (at(i, "<=") || at(i, ">=")) && spaced(i, 2):
			add("boundary", i, i+2, src[i:i+1])
			i++
		case (at(i, "<") || at(i, ">")) && spaced(i, 1):
			add("boundary", i, i+1, src[i:i+1]+"=")
		case l != langPy && at(i, "!") && !at(i+1, "="):
			p := prevSig(i)
			if p == 'x' || p != 0 && (isIdent(p) || p == ')' || p == ']') {
				continue
			}
			add("negation", i, i+1, "")
		case l == langPy && (wordAt(i, "and") || wordAt(i, "or")):
			w, repl := "and", "or"
			if src[i] == 'o' {
				w, repl = "or", "and"
			}
			add("logical", i, i+len(w), repl)
			i += len(w) - 1
		case l == langPy && wordAt(i, "is"):
			j := i + 2
			if strings.HasPrefix(src[j:], " not ") {
				add("equality", i, j+4, "is")
				i = j + 3
			} else {
				add("equality", i, j, "is not")
				i++
			}
		case l == langPy && wordAt(i, "not"):
			if strings.HasPrefix(src[i+3:], " in ") || i >= 3 && strings.HasSuffix(src[:i], "is ") {
				i += 2
				continue
			}
			end := i + 3
			for end < len(src) && src[end] == ' ' {
				end++
			}
			add("negation", i, end, "")
			i += 2
		case wordAt(i, trueWord):
			add("boolean", i, i+len(trueWord), falseWord)
			i += len(trueWord) - 1
		case wordAt(i, falseWord):
			add("boolean", i, i+len(falseWord), trueWord)
			i += len(falseWord) - 1
		case wordAt(i, "if") || l == langPy && wordAt(i, "elif"):
			kw := 2
			if src[i] == 'e' {
				kw = 4
			}
			if l == langPy && !firstOnLine(i) {
				i += kw - 1
				continue
			}
			if s, e, ok := condition(src, i+kw, l, code); ok {
				add("guard", s, e, falseWord+" "+andWord+" ("+src[s:e]+")")
			}
			i += kw - 1
		}
	}
	return out
}

// condition returns the extent of the condition of the if statement whose
// keyword ends at i.
func condition(src string, i int, l lang, code func(int) bool) (int, int, bool) {
	for i < len(src) && (src[i] == ' ' || src[i] == '\t') {
		i++
	}
	switch l {
	case langTS, langKotlin:
		if i >= len(src) || src[i] != '(' {
			return 0, 0, false
		}
		depth := 0
		for j := i; j < len(src); j++ {
			if !code(j) {
				continue
			}
			switch src[j] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return i + 1, j, true
				}
			}
		}
	case langGo, langPy:
		start, depth := i, 0
		for j := i; j < len(src); j++ {
			if !code(j) {
				continue
			}
			switch c := src[j]; {
			case c == '(' || c == '[' || l == langPy && c == '{':
				depth++
			case c == ')' || c == ']' || l == langPy && c == '}':
				depth--
			case depth > 0:
			case l == langGo && c == ';':
				start = j + 1
				for start < len(src) && src[start] == ' ' {
					start++
				}
			case l == langGo && c == '{':
				end := len(strings.TrimRight(src[:j], " "))
				return start, end, end > start
			case l == langPy && c == ':' && (j+1 >= len(src) || src[j+1] != '='):
				return start, j, true
			}
		}
	}
	return 0, 0, false
}
