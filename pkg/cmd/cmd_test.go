package cmd

import (
	"testing"
	"time"

	"github.com/elixirhealth/service-base/pkg/cmd"
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
	courier := "127.0.0.1:10100"
	catalog := "127.0.0.1:10200"
	directory := "127.0.0.1:10300"
	user := "127.0.0.1:10400"
	parallelism := uint(2)
	timeout := 2 * time.Second

	viper.Set(cmd.ServerPortFlag, serverPort)
	viper.Set(cmd.MetricsPortFlag, metricsPort)
	viper.Set(cmd.ProfilerPortFlag, profilerPort)
	viper.Set(cmd.LogLevelFlag, logLevel)
	viper.Set(cmd.ProfileFlag, profile)
	viper.Set(courierFlag, courier)
	viper.Set(catalogFlag, catalog)
	viper.Set(directoryFlag, directory)
	viper.Set(userFlag, user)
	viper.Set(parallelismFlag, parallelism)
	viper.Set(timeoutFlag, timeout)

	c, err := getTimelineConfig()
	assert.Nil(t, err)
	assert.Equal(t, serverPort, c.ServerPort)
	assert.Equal(t, metricsPort, c.MetricsPort)
	assert.Equal(t, profilerPort, c.ProfilerPort)
	assert.Equal(t, logLevel, c.LogLevel.String())
	assert.Equal(t, profile, c.Profile)
	assert.Equal(t, courier, c.Courier.String())
	assert.Equal(t, catalog, c.Catalog.String())
	assert.Equal(t, directory, c.Directory.String())
	assert.Equal(t, user, c.User.String())
	assert.Equal(t, parallelism, c.Parallelism)
	assert.Equal(t, timeout, c.RequestTimeout)
}
