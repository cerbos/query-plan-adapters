package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kit build|record|summary|report [args]")
		os.Exit(2)
	}
	repo, err := filepath.Abs("../..")
	must(err)
	root := filepath.Join(repo, "domains")
	doms := domainDirs(root, os.Args[2:])
	switch os.Args[1] {
	case "build":
		for _, d := range doms {
			must(build(d))
		}
	case "record":
		for _, d := range doms {
			must(build(d))
			must(record(context.Background(), repo, d))
			fmt.Println("recorded", filepath.Base(d))
		}
	case "summary":
		for _, d := range doms {
			must(summary(d))
		}
	case "report":
		must(report(root))
	default:
		fmt.Fprintln(os.Stderr, "unknown command", os.Args[1])
		os.Exit(2)
	}
}

func domainDirs(root string, args []string) []string {
	if len(args) > 0 {
		var out []string
		for _, a := range args {
			out = append(out, filepath.Join(root, a))
		}
		return out
	}
	entries, err := os.ReadDir(root)
	must(err)
	var out []string
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(root, e.Name(), "schema.yaml")); err == nil {
			out = append(out, filepath.Join(root, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

func build(dir string) error {
	s, err := loadSchema(dir)
	if err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	d, err := loadData(dir, s)
	if err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	res, err := project(s, d)
	if err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	gen := filepath.Join(dir, "generated")
	if err := writeJSON(filepath.Join(gen, "resources.json"), map[string]any{"kind": s.Kind, "nulls": s.Nulls, "resources": res}); err != nil {
		return err
	}
	rows, err := normalizedRows(s, d)
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(gen, "rows.json"), rows); err != nil {
		return err
	}
	for _, dl := range dialects {
		sql, err := renderSQL(dl, s, d)
		if err != nil {
			return fmt.Errorf("%s %s: %w", dir, dl, err)
		}
		if err := os.MkdirAll(filepath.Join(gen, "sql"), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(gen, "sql", dl+".sql"), []byte(sql), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
