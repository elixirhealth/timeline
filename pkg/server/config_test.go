package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewDefaultConfig(t *testing.T) {
	c := NewDefaultConfig()
	assert.NotNil(t, c)
	assert.NotEmpty(t, c.RequestTimeout)
	assert.NotEmpty(t, c.Parallelism)
}

func TestConfig_MarshalLogObject(t *testing.T) {
	oe := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
	c := NewDefaultConfig()
	err := c.MarshalLogObject(oe)
	assert.Nil(t, err)
}

func TestConfig_WithParallelism(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultParallelism()
	assert.Equal(t, c1.Parallelism, c2.WithParallelism(0).Parallelism)
	assert.NotEqual(t, c1.Parallelism, c3.WithParallelism(2).Parallelism)
}

func TestConfig_WithRequestTimeout(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultRequestTimeout()
	assert.Equal(t, c1.RequestTimeout, c2.WithRequestTimeout(0).RequestTimeout)
	assert.NotEqual(t, c1.RequestTimeout,
		c3.WithRequestTimeout(2*time.Second).RequestTimeout)
}
