package cmd

import (
	"testing"

	"github.com/elxirhealth/service-base/pkg/cmd"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestGetTimelineConfig(t *testing.T) {
	serverPort := uint(1234)
	metricsPort := uint(5678)
	profilerPort := uint(9012)
	logLevel := zapcore.DebugLevel.String()
	profile := true
	// TODO add other non-default config values

	viper.Set(cmd.ServerPortFlag, serverPort)
	viper.Set(cmd.MetricsPortFlag, metricsPort)
	viper.Set(cmd.ProfilerPortFlag, profilerPort)
	viper.Set(cmd.LogLevelFlag, logLevel)
	viper.Set(cmd.ProfileFlag, profile)
	// TODO set other non-default config value

	c, err := getTimelineConfig()
	assert.Nil(t, err)
	assert.Equal(t, serverPort, c.ServerPort)
	assert.Equal(t, metricsPort, c.MetricsPort)
	assert.Equal(t, profilerPort, c.ProfilerPort)
	assert.Equal(t, logLevel, c.LogLevel.String())
	assert.Equal(t, profile, c.Profile)
	// TODO assert equal other non-default config values

}
