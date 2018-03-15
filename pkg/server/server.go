package server

import (
	"encoding/hex"

	"github.com/elxirhealth/service-base/pkg/server"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var (
	ErrMissingAuthorEntityID = errors.New("missing author entity ID")
	ErrDocNotEnvelope        = errors.New("document unexpectedly not an Envelope")
	ErrDocNotEntry           = errors.New("document unexpectedly not an Entry")
)

// Timeline implements the TimelineServer interface.
type Timeline struct {
	*server.BaseServer
	config *Config

	entityIDGetter      entityIDGetter
	pubReceiptGetter    pubReceiptGetter
	envelopeGetter      envelopeGetter
	entryMetadataGetter entryMetadataGetter
	entitySummaryGetter entitySummaryGetter
}

// newTimeline creates a new TimelineServer from the given config.
func newTimeline(config *Config) (*Timeline, error) {
	baseServer := server.NewBaseServer(config.BaseConfig)
	return &Timeline{
		BaseServer: baseServer,
		config:     config,
	}, nil
}

func (t *Timeline) Get(
	ctx context.Context, rq *api.GetRequest,
) (*api.GetResponse, error) {
	if err := api.ValidateGetRequest(rq); err != nil {
		return nil, err
	}
	limit := rq.Limit
	if limit == 0 {
		limit = api.MaxLimit
	}

	readerEntityIDs, err := t.entityIDGetter.get(rq.UserId)
	if err != nil {
		return nil, err
	}
	prs, err := t.pubReceiptGetter.get(readerEntityIDs, rq.TimeRange, limit)
	if err != nil {
		return nil, err
	}
	envs, err := t.envelopeGetter.get(prs)
	if err != nil {
		return nil, err
	}
	entryMetas, err := t.entryMetadataGetter.get(prs)
	if err != nil {
		return nil, err
	}
	entitySummaries, err := t.entitySummaryGetter.get(prs)
	if err != nil {
		return nil, err
	}
	// stitch it all together
	events := make([]*api.Event, len(prs))
	for i, pr := range prs {
		entryKeyHex := hex.EncodeToString(pr.EntryKey)
		events[i] = &api.Event{
			Time:          pr.ReceivedTime,
			Envelope:      envs[i],
			EntryMetadata: entryMetas[entryKeyHex],
			Reader:        entitySummaries[pr.ReaderEntityId],
			Author:        entitySummaries[pr.AuthorEntityId],
		}
	}

	return &api.GetResponse{Events: events}, nil
}
