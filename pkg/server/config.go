package server

import (
	"net"
	"time"

	"github.com/drausin/libri/libri/common/errors"
	"github.com/elxirhealth/service-base/pkg/server"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultParallelism is the default parallelism used for dependency requests.
	DefaultParallelism = uint(4)

	// DefaultRequestTimeout is the default time for requests made to dependency services.
	DefaultRequestTimeout = 1 * time.Second
)

// Config is the config for a Timeline instance.
type Config struct {
	*server.BaseConfig

	RequestTimeout time.Duration
	Parallelism    uint

	Courier   *net.TCPAddr
	Catalog   *net.TCPAddr
	Directory *net.TCPAddr
	User      *net.TCPAddr
}

// NewDefaultConfig create a new config instance with default values.
func NewDefaultConfig() *Config {
	config := &Config{
		BaseConfig: server.NewDefaultBaseConfig(),
	}
	return config.
		WithDefaultParallelism().
		WithDefaultRequestTimeout()
}

// MarshalLogObject writes the config to the given object encoder.
func (c *Config) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	err := c.BaseConfig.MarshalLogObject(oe)
	errors.MaybePanic(err) // should never happen

	oe.AddString(logCourier, c.Courier.String())
	oe.AddString(logCatalog, c.Catalog.String())
	oe.AddString(logDirectory, c.Directory.String())
	oe.AddString(logUser, c.User.String())
	return nil
}

// WithParallelism sets the parallelism used by each getter.
func (c *Config) WithParallelism(n uint) *Config {
	if n == 0 {
		return c.WithDefaultParallelism()
	}
	c.Parallelism = n
	return c
}

// WithDefaultParallelism sets the getter parallelism to the default value.
func (c *Config) WithDefaultParallelism() *Config {
	c.Parallelism = DefaultParallelism
	return c
}

// WithRequestTimeout sets the key Get request timeout to the given value or to the default
// if it is zero-valued.
func (c *Config) WithRequestTimeout(t time.Duration) *Config {
	if t == 0 {
		return c.WithDefaultRequestTimeout()
	}
	c.RequestTimeout = t
	return c
}

// WithDefaultRequestTimeout sets the key request timeout to the default value.
func (c *Config) WithDefaultRequestTimeout() *Config {
	c.RequestTimeout = DefaultRequestTimeout
	return c
}

// WithCourierAddr sets the catalog address to the given value.
func (c *Config) WithCourierAddr(addr *net.TCPAddr) *Config {
	c.Courier = addr
	return c
}

// WithCatalogAddr sets the catalog addresses to the given value.
func (c *Config) WithCatalogAddr(addr *net.TCPAddr) *Config {
	c.Catalog = addr
	return c
}

// WithDirectoryAddr sets the directory address to the given value.
func (c *Config) WithDirectoryAddr(addr *net.TCPAddr) *Config {
	c.Directory = addr
	return c
}

// WithUserAddr sets the user address to the given value.
func (c *Config) WithUserAddr(addr *net.TCPAddr) *Config {
	c.User = addr
	return c
}
