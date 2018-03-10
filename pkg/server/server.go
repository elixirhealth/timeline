package server

import (
	"github.com/elxirhealth/service-base/pkg/server"
	"github.com/elxirhealth/timeline/pkg/server/storage"
)

// Timeline implements the TimelineServer interface.
type Timeline struct {
	*server.BaseServer
	config *Config

	storer storage.Storer
	// TODO maybe add other things here
}

// newTimeline creates a new TimelineServer from the given config.
func newTimeline(config *Config) (*Timeline, error) {
	baseServer := server.NewBaseServer(config.BaseConfig)
	storer, err := getStorer(config, baseServer.Logger)
	if err != nil {
		return nil, err
	}
	// TODO maybe add other init

	return &Timeline{
		BaseServer: baseServer,
		config:     config,
		storer:     storer,
		// TODO maybe add other things
	}, nil
}

// TODO implement timelineapi.Timeline endpoints
