package server

import (
	"container/heap"
	"encoding/hex"

	libriapi "github.com/drausin/libri/libri/librarian/api"
	catapi "github.com/elxirhealth/catalog/pkg/catalogapi"
	"github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/elxirhealth/directory/pkg/directoryapi"
	"github.com/elxirhealth/key/pkg/keyapi"
	"github.com/elxirhealth/service-base/pkg/server"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	"github.com/elxirhealth/user/pkg/userapi"
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

	courier   courierapi.CourierClient
	catalog   catapi.CatalogClient
	key       keyapi.KeyClient
	directory directoryapi.DirectoryClient
	user      userapi.UserClient
}

// newTimeline creates a new TimelineServer from the given config.
func newTimeline(config *Config) (*Timeline, error) {
	baseServer := server.NewBaseServer(config.BaseConfig)
	// TODO maybe add other init

	return &Timeline{
		BaseServer: baseServer,
		config:     config,
		// TODO maybe add other things
	}, nil
}

func (t *Timeline) Get(
	ctx context.Context, rq *api.GetRequest,
) (*api.GetResponse, error) {
	if err := api.ValidateGetRequest(rq); err == nil {
		return nil, err
	}
	limit := rq.Limit
	if limit == 0 {
		limit = api.MaxLimit
	}

	readerEntityIDs, err := t.getUserEntityIDs(rq.UserId)
	if err != nil {
		return nil, err
	}
	prs, err := t.getReaderPublications(readerEntityIDs, rq.TimeRange, limit)
	if err != nil {
		return nil, err
	}
	entryMetas, err := t.getEntryMetas(prs)
	if err != nil {
		return nil, err
	}
	envs, err := t.getEnvelopes(prs)
	if err != nil {
		return nil, err
	}
	entitySummaries, err := t.getEntitySummaries(prs)
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

func (t *Timeline) getUserEntityIDs(userID string) ([]string, error) {
	rq := &userapi.GetEntitiesRequest{UserId: userID}
	ctx, cancel := context.WithTimeout(context.Background(), t.config.RequestTimeout)
	defer cancel()
	rp, err := t.user.GetEntities(ctx, rq)
	if err != nil {
		return nil, err
	}
	return rp.EntityIds, nil
}

func (t *Timeline) getReaderPublications(
	entityIDs []string, tr *api.TimeRange, limit uint32,
) ([]*catapi.PublicationReceipt, error) {
	prHeap := &publicationReceipts{}
	for _, readerEntityID := range entityIDs { // TODO parallelize
		readerPRs, err := t.getReaderEntityPublications(readerEntityID, tr, limit)
		if err != nil {
			return nil, err
		}
		for _, readerPR := range readerPRs {
			if readerPR.AuthorEntityId == "" {
				return nil, ErrMissingAuthorEntityID
			}
			heap.Push(prHeap, readerPR)
			if prHeap.Len() > int(limit) {
				heap.Pop(prHeap)
			}
		}
	}

	// create list of PRs, sorted descending by received time
	prs := make([]*catapi.PublicationReceipt, prHeap.Len())
	for i := len(prs) - 1; i >= 0; i-- {
		prs[i] = heap.Pop(prHeap).(*catapi.PublicationReceipt)
	}
	return prs, nil
}

func (t *Timeline) getReaderEntityPublications(
	entityID string, tr *api.TimeRange, limit uint32,
) ([]*catapi.PublicationReceipt, error) {
	rq := &catapi.SearchRequest{
		ReaderEntityId: entityID,
		AuthorEntityId: "",
		After:          tr.LowerBound,
		Before:         tr.UpperBound,
		Limit:          limit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.config.RequestTimeout)
	rp, err := t.catalog.Search(ctx, rq)
	cancel()
	if err != nil {
		return nil, err
	}
	return rp.Result, nil
}

func (t *Timeline) getEnvelopes(prs []*catapi.PublicationReceipt) ([]*libriapi.Envelope, error) {
	envs := make([]*libriapi.Envelope, len(prs))
	for i, pr := range prs { // TODO parallelize
		env, err := t.getEnvelope(pr.EnvelopeKey)
		if err != nil {
			return nil, err
		}
		envs[i] = env
	}
	return envs, nil
}

func (t *Timeline) getEnvelope(key []byte) (*libriapi.Envelope, error) {
	doc, err := t.getDocument(key)
	if err != nil {
		return nil, err
	}
	env, ok := doc.Contents.(*libriapi.Document_Envelope)
	if !ok {
		return nil, ErrDocNotEnvelope
	}
	return env.Envelope, nil
}

func (t *Timeline) getEntryMetas(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntryMetadata, error) {
	entryKeys := make(map[string][]byte)
	for _, pr := range prs {
		entryKeyHex := hex.EncodeToString(pr.EntryKey)
		if _, in := entryKeys[entryKeyHex]; !in {
			entryKeys[entryKeyHex] = pr.EntryKey
		}
	}
	entryMetas := make(map[string]*api.EntryMetadata)
	for entryKeyHex, entryKey := range entryKeys { // TODO parallelize
		entryMeta, err := t.getEntryMeta(entryKey)
		if err != nil {
			return nil, err
		}
		entryMetas[entryKeyHex] = entryMeta
	}
	return entryMetas, nil
}

func (t *Timeline) getEntryMeta(key []byte) (*api.EntryMetadata, error) {
	doc, err := t.getDocument(key)
	if err != nil {
		return nil, err
	}
	entry, ok := doc.Contents.(*libriapi.Document_Entry)
	if !ok {
		return nil, ErrDocNotEntry
	}
	return &api.EntryMetadata{
		CreatedTime:           entry.Entry.CreatedTime,
		MetadataCiphertext:    entry.Entry.MetadataCiphertext,
		MetadataCiphertextMac: entry.Entry.MetadataCiphertextMac,
	}, nil
}

func (t *Timeline) getDocument(key []byte) (*libriapi.Document, error) {
	rq := &courierapi.GetRequest{Key: key}
	ctx, cancel := context.WithTimeout(context.Background(), t.config.RequestTimeout)
	rp, err := t.courier.Get(ctx, rq)
	cancel()
	if err != nil {
		return nil, err
	}
	return rp.Value, nil
}

func (t *Timeline) getEntitySummaries(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntitySummary, error) {
	entityIDs := make(map[string]struct{})
	for _, pr := range prs {
		entityIDs[pr.ReaderEntityId] = struct{}{}
		entityIDs[pr.AuthorEntityId] = struct{}{}
	}
	entitySummaries := make(map[string]*api.EntitySummary)
	for entityID := range entityIDs { // TODO parallelize
		sum, err := t.getEntitySummary(entityID)
		if err != nil {
			return nil, err
		}
		entitySummaries[entityID] = sum
	}
	return entitySummaries, nil
}

func (t *Timeline) getEntitySummary(entityID string) (*api.EntitySummary, error) {
	rq := &directoryapi.GetEntityRequest{EntityId: entityID}
	ctx, cancel := context.WithTimeout(context.Background(), t.config.RequestTimeout)
	rp, err := t.directory.GetEntity(ctx, rq)
	cancel()
	if err != nil {
		return nil, err
	}
	return &api.EntitySummary{
		EntityId: entityID,
		Type:     rp.Entity.Type(),
		Name:     "", // TODO rp.Entity.Name()
	}, nil
}
