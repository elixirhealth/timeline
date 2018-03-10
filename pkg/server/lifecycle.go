package server

import (
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	"google.golang.org/grpc"
)

// Start starts the server and eviction routines.
func Start(config *Config, up chan *Timeline) error {
	c, err := newTimeline(config)
	if err != nil {
		return err
	}

	// start Timeline aux routines
	// TODO add go x.auxRoutine() or delete comment

	registerServer := func(s *grpc.Server) { api.RegisterTimelineServer(s, c) }
	return c.Serve(registerServer, func() { up <- c })
}
