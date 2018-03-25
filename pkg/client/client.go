package client

import (
	api "github.com/elixirhealth/timeline/pkg/timelineapi"
	"google.golang.org/grpc"
)

// NewInsecure returns a new TimelineClient without any TLS on the connection.
func NewInsecure(address string) (api.TimelineClient, error) {
	cc, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return api.NewTimelineClient(cc), nil
}
