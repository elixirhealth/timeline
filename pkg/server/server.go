package server

import (
	"encoding/hex"

	catclient "github.com/elxirhealth/catalog/pkg/client"
	courclient "github.com/elxirhealth/courier/pkg/client"
	dirclient "github.com/elxirhealth/directory/pkg/client"
	"github.com/elxirhealth/service-base/pkg/server"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	userclient "github.com/elxirhealth/user/pkg/client"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

var (
	// ErrMissingAuthorEntityID denotes when a publication is unexpectedly missing the author
	// entity ID.
	ErrMissingAuthorEntityID = errors.New("missing author entity ID")

	// ErrDocNotEnvelope denotes when a publication is unexpectedly not an envelope.
	ErrDocNotEnvelope = errors.New("document unexpectedly not an Envelope")

	// ErrDocNotEntry denotes when a document is unexpectedly not an entry.
	ErrDocNotEntry = errors.New("document unexpectedly not an Entry")

	errMissingCatalog   = errors.New("missing catalog address")
	errMissingCourier   = errors.New("missing courier address")
	errMissingDirectory = errors.New("missing directory address")
	errMissingUser      = errors.New("missing user address")
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

	if config.Courier == nil {
		return nil, errMissingCourier
	}
	courier, err := courclient.NewInsecure(config.Courier.String())
	if err != nil {
		return nil, err
	}
	envelopeGetter := &envelopeGetterImpl{
		lg:          baseServer.Logger,
		rqTimeout:   config.RequestTimeout,
		parallelism: config.Parallelism,
		courier:     courier,
	}
	entryMetadataGetter := &entryMetadataGetterImpl{
		lg:          baseServer.Logger,
		rqTimeout:   config.RequestTimeout,
		parallelism: config.Parallelism,
		courier:     courier,
	}

	if config.Catalog == nil {
		return nil, errMissingCatalog
	}
	catalog, err := catclient.NewInsecure(config.Catalog.String())
	if err != nil {
		return nil, err
	}
	pubReceiptGetter := &pubReceiptGetterImpl{
		lg:          baseServer.Logger,
		rqTimeout:   config.RequestTimeout,
		parallelism: config.Parallelism,
		catalog:     catalog,
	}

	if config.Directory == nil {
		return nil, errMissingDirectory
	}
	directory, err := dirclient.NewInsecure(config.Directory.String())
	if err != nil {
		return nil, err
	}
	entitySummaryGetter := &entitySummaryGetterImpl{
		lg:          baseServer.Logger,
		rqTimeout:   config.RequestTimeout,
		parallelism: config.Parallelism,
		directory:   directory,
	}

	if config.User == nil {
		return nil, errMissingUser
	}
	user, err := userclient.NewInsecure(config.User.String())
	if err != nil {
		return nil, err
	}
	entityIDGetter := &entityIDGetterImpl{
		lg:        baseServer.Logger,
		rqTimeout: config.RequestTimeout,
		user:      user,
	}

	return &Timeline{
		BaseServer: baseServer,
		config:     config,

		entityIDGetter:      entityIDGetter,
		pubReceiptGetter:    pubReceiptGetter,
		envelopeGetter:      envelopeGetter,
		entryMetadataGetter: entryMetadataGetter,
		entitySummaryGetter: entitySummaryGetter,
	}, nil
}

// Get returns a timeline of events for a given user.
func (t *Timeline) Get(
	ctx context.Context, rq *api.GetRequest,
) (*api.GetResponse, error) {
	t.Logger.Debug("received Get request", zap.String("user_id", rq.UserId))
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
	t.Logger.Info("got events", zap.String("user_id", rq.UserId), zap.Int("n_events", len(events)))
	return &api.GetResponse{Events: events}, nil
}
