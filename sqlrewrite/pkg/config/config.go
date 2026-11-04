// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"strings"

	"braces.dev/errtrace"
	"go.uber.org/config"
)

type Flags struct {
	Path string   `help:"Path to config file" type:"existingfile" placeholder:"./config.yaml" env:"SQLREWRITE_CONFIG"`
	Set  []string `help:"Config overrides as key=value, applied after the file" placeholder:"server.listenAddr=:3594"`
}

type Section interface {
	Key() string
}

type Defaulter interface {
	SetDefaults()
}

type Validator interface {
	Validate() error
}

type Provider struct {
	provider config.Provider
}

func Load(flags Flags) (*Provider, error) {
	var sources []config.YAMLOption
	if flags.Path != "" {
		finfo, err := os.Stat(flags.Path)
		if err != nil {
			return nil, errtrace.Errorf("failed to stat %s: %w", flags.Path, err)
		}
		if finfo.IsDir() {
			return nil, errtrace.Errorf("config file path is a directory: %s", flags.Path)
		}
		sources = append(sources, config.File(flags.Path))
	}

	overrides := map[string]any{}
	for _, s := range flags.Set {
		key, value, ok := strings.Cut(s, "=")
		if !ok || key == "" {
			return nil, errtrace.Errorf("config override %q is not key=value", s)
		}
		setPath(overrides, strings.Split(key, "."), value)
	}
	sources = append(sources, config.Static(overrides), config.Expand(os.LookupEnv))

	p, err := config.NewYAML(sources...)
	if err != nil {
		return nil, errtrace.Errorf("failed to load config: %w", err)
	}
	return &Provider{provider: p}, nil
}

func FromMap(m map[string]any) (*Provider, error) {
	p, err := config.NewYAML(config.Static(m))
	if err != nil {
		return nil, errtrace.Errorf("failed to load config: %w", err)
	}
	return &Provider{provider: p}, nil
}

func (p *Provider) GetSection(section Section) error {
	if d, ok := section.(Defaulter); ok {
		d.SetDefaults()
	}
	if err := p.provider.Get(section.Key()).Populate(section); err != nil {
		return errtrace.Errorf("failed to read config section %s: %w", section.Key(), err)
	}
	if v, ok := section.(Validator); ok {
		if err := v.Validate(); err != nil {
			return errtrace.Errorf("invalid config section %s: %w", section.Key(), err)
		}
	}
	return nil
}

func setPath(m map[string]any, path []string, value string) {
	for _, k := range path[:len(path)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[k] = next
		}
		m = next
	}
	m[path[len(path)-1]] = value
}
