// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"os"
	"strings"

	isatty "github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
)

type Flags struct {
	Level string `name:"level" help:"Log level (${enum})" default:"info" enum:"trace,debug,info,warn,error" env:"SQLREWRITE_LOG_LEVEL"`
}

func New(flags Flags) zerolog.Logger {
	level, err := zerolog.ParseLevel(strings.ToLower(flags.Level))
	if err != nil {
		level = zerolog.InfoLevel
	}

	var logger zerolog.Logger
	if isatty.IsTerminal(os.Stderr.Fd()) {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		logger = zerolog.New(os.Stderr)
	}
	return logger.Level(level).With().Timestamp().Logger()
}
