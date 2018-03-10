package server

import (
	"github.com/drausin/libri/libri/common/errors"
	"github.com/elxirhealth/service-base/pkg/server"
	"github.com/elxirhealth/timeline/pkg/server/storage"
	"go.uber.org/zap/zapcore"
)

const (
// TODO add default config values here
)

// Config is the config for a Timeline instance.
type Config struct {
	*server.BaseConfig
	Storage *storage.Parameters
	// TODO add config elements
}

// NewDefaultConfig create a new config instance with default values.
func NewDefaultConfig() *Config {
	config := &Config{
		BaseConfig: server.NewDefaultBaseConfig(),
	}
	return config.
		WithDefaultStorage()
	// TODO add .WithDefaultCONFIGELEMENT for each CONFIGELEMENT
}

// MarshalLogObject writes the config to the given object encoder.
func (c *Config) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	err := c.BaseConfig.MarshalLogObject(oe)
	errors.MaybePanic(err) // should never happen
	err = oe.AddObject(logStorage, c.Storage)
	errors.MaybePanic(err) // should never happen

	// TODO add other config elements
	return nil
}

// WithStorage sets the cache parameters to the given value or the defaults if it is nil.
func (c *Config) WithStorage(p *storage.Parameters) *Config {
	if p == nil {
		return c.WithDefaultStorage()
	}
	c.Storage = p
	return c
}

// WithDefaultStorage set the Cache parameters to their default values.
func (c *Config) WithDefaultStorage() *Config {
	c.Storage = storage.NewDefaultParameters()
	return c
}

// TODO add WithCONFIGELEMENT and WithDefaultCONFIGELEMENT methods for each CONFIGELEMENT
