package cmd

import (
	"errors"
	"log"
	"net"

	cerrors "github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	"github.com/elixirhealth/service-base/pkg/cmd"
	bserver "github.com/elixirhealth/service-base/pkg/server"
	"github.com/elixirhealth/timeline/pkg/server"
	"github.com/elixirhealth/timeline/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	serviceNameLower = "timeline"
	serviceNameCamel = "Timeline"
	envVarPrefix     = "TIMELINE"
	logLevelFlag     = "logLevel"
	courierFlag      = "courier"
	catalogFlag      = "catalog"
	directoryFlag    = "directory"
	userFlag         = "user"
	timeoutFlag      = "timeout"
	parallelismFlag  = "parallelism"
)

var (
	rootCmd = &cobra.Command{
		Short: "operate a timeline server",
	}

	errMissingCatalog   = errors.New("missing catalog address")
	errMissingCourier   = errors.New("missing courier address")
	errMissingDirectory = errors.New("missing directory address")
	errMissingUser      = errors.New("missing user address")
)

func init() {
	rootCmd.PersistentFlags().String(logLevelFlag, bserver.DefaultLogLevel.String(),
		"log level")

	cmd.Start(serviceNameLower, serviceNameCamel, rootCmd, version.Current, start,
		func(flags *pflag.FlagSet) {
			flags.String(courierFlag, "", "courier service address")
			flags.String(catalogFlag, "", "catalog service address")
			flags.String(directoryFlag, "", "directory service address")
			flags.String(userFlag, "", "user service address")
			flags.Duration(timeoutFlag, server.DefaultRequestTimeout,
				"timeout for dependency service requests")
			flags.Uint(parallelismFlag, server.DefaultParallelism,
				"parallelism for dependency service requests")
		})

	testCmd := cmd.Test(serviceNameLower, rootCmd)
	cmd.TestHealth(serviceNameLower, testCmd)
	cmd.Version(serviceNameLower, rootCmd, version.Current)

	// bind viper flags
	viper.SetEnvPrefix(envVarPrefix) // look for env vars with prefix
	viper.AutomaticEnv()             // read in environment variables that match
	cerrors.MaybePanic(viper.BindPFlags(rootCmd.Flags()))
}

// Execute runs the root timeline command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func start() error {
	config, err := getTimelineConfig()
	if err != nil {
		return err
	}
	return server.Start(config, make(chan *server.Timeline, 1))
}

func getTimelineConfig() (*server.Config, error) {
	courierAddr, err := getAddr(courierFlag, errMissingCourier)
	if err != nil {
		return nil, err
	}
	catalogAddr, err := getAddr(catalogFlag, errMissingCatalog)
	if err != nil {
		return nil, err
	}
	directoryAddr, err := getAddr(directoryFlag, errMissingDirectory)
	if err != nil {
		return nil, err
	}
	userAddr, err := getAddr(userFlag, errMissingUser)
	if err != nil {
		return nil, err
	}

	c := server.NewDefaultConfig()
	c.WithServerPort(uint(viper.GetInt(cmd.ServerPortFlag))).
		WithMetricsPort(uint(viper.GetInt(cmd.MetricsPortFlag))).
		WithProfilerPort(uint(viper.GetInt(cmd.ProfilerPortFlag))).
		WithLogLevel(logging.GetLogLevel(viper.GetString(logLevelFlag))).
		WithProfile(viper.GetBool(cmd.ProfileFlag))
	c.WithCourierAddr(courierAddr).
		WithCatalogAddr(catalogAddr).
		WithDirectoryAddr(directoryAddr).
		WithUserAddr(userAddr).
		WithParallelism(uint(viper.GetInt(parallelismFlag))).
		WithRequestTimeout(viper.GetDuration(timeoutFlag))

	return c, nil
}

func getAddr(addrFlag string, missingErr error) (*net.TCPAddr, error) {
	strAddr := viper.GetString(addrFlag)
	if strAddr == "" {
		return nil, missingErr
	}
	return net.ResolveTCPAddr("tcp4", strAddr)
}
