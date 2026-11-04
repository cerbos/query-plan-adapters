// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"errors"
	"time"
)

const (
	confKey                = "server"
	defaultListenAddr      = ":3594"
	defaultShutdownTimeout = 10 * time.Second
	defaultReadTimeout     = 30 * time.Second
	defaultMaxRequestBytes = 4 << 20
)

type Conf struct {
	ListenAddr      string        `yaml:"listenAddr"`
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`
	ReadTimeout     time.Duration `yaml:"readTimeout"`
	MaxRequestBytes int64         `yaml:"maxRequestBytes"`
}

func (c *Conf) Key() string { return confKey }

func (c *Conf) SetDefaults() {
	c.ListenAddr = defaultListenAddr
	c.ShutdownTimeout = defaultShutdownTimeout
	c.ReadTimeout = defaultReadTimeout
	c.MaxRequestBytes = defaultMaxRequestBytes
}

func (c *Conf) Validate() error {
	if c.ListenAddr == "" {
		return errors.New("listenAddr is required")
	}
	if c.MaxRequestBytes <= 0 {
		return errors.New("maxRequestBytes must be positive")
	}
	return nil
}
