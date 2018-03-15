package server

import (
	"container/heap"
	"encoding/hex"
	"time"

	libriapi "github.com/drausin/libri/libri/librarian/api"
	catapi "github.com/elxirhealth/catalog/pkg/catalogapi"
	"github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/elxirhealth/directory/pkg/directoryapi"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	"github.com/elxirhealth/user/pkg/userapi"
	"golang.org/x/net/context"
)

type entityIDGetter interface {
	get(userID string) ([]string, error)
}

type entityIDGetterImpl struct {
	rqTimeout time.Duration
	user      userapi.UserClient
}

func (g *entityIDGetterImpl) get(userID string) ([]string, error) {
	rq := &userapi.GetEntitiesRequest{UserId: userID}
	ctx, cancel := context.WithTimeout(context.Background(), g.rqTimeout)
	defer cancel()
	rp, err := g.user.GetEntities(ctx, rq)
	if err != nil {
		return nil, err
	}
	return rp.EntityIds, nil
}

type pubReceiptGetter interface {
	get(
		entityIDs []string, tr *api.TimeRange, limit uint32,
	) ([]*catapi.PublicationReceipt, error)
}

type pubReceiptGetterImpl struct {
	rqTimeout time.Duration
	catalog   catapi.CatalogClient
}

func (g *pubReceiptGetterImpl) get(
	entityIDs []string, tr *api.TimeRange, limit uint32,
) ([]*catapi.PublicationReceipt, error) {
	prHeap := &publicationReceipts{}
	for _, readerEntityID := range entityIDs { // TODO parallelize
		rq := &catapi.SearchRequest{
			ReaderEntityId: readerEntityID,
			AuthorEntityId: "",
			After:          tr.LowerBound,
			Before:         tr.UpperBound,
			Limit:          limit,
		}

		ctx, cancel := context.WithTimeout(context.Background(), g.rqTimeout)
		rp, err := g.catalog.Search(ctx, rq)
		cancel()
		if err != nil {
			return nil, err
		}
		for _, readerPR := range rp.Result {
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

type envelopeGetter interface {
	get(prs []*catapi.PublicationReceipt) ([]*libriapi.Envelope, error)
}

type envelopeGetterImpl struct {
	rqTimeout time.Duration
	courier   courierapi.CourierClient
}

func (g *envelopeGetterImpl) get(prs []*catapi.PublicationReceipt) ([]*libriapi.Envelope, error) {
	envs := make([]*libriapi.Envelope, len(prs))
	for i, pr := range prs { // TODO parallelize
		rq := &courierapi.GetRequest{Key: pr.EnvelopeKey}
		ctx, cancel := context.WithTimeout(context.Background(), g.rqTimeout)
		rp, err := g.courier.Get(ctx, rq)
		cancel()
		if err != nil {
			return nil, err
		}
		env, ok := rp.Value.Contents.(*libriapi.Document_Envelope)
		if !ok {
			return nil, ErrDocNotEnvelope
		}
		envs[i] = env.Envelope
	}
	return envs, nil
}

type entryMetadataGetter interface {
	// get returns the (encrypted) entry metadata (keyed by entry key hex) for all entries in
	// the list of publication receipts.
	get(prs []*catapi.PublicationReceipt) (map[string]*api.EntryMetadata, error)
}

type entryMetadataGetterImpl struct {
	rqTimeout time.Duration
	courier   courierapi.CourierClient
}

func (g *entryMetadataGetterImpl) get(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntryMetadata, error) {

	// get unique set of keys to avoid unnecessary requests below for repeats
	entryKeys := make(map[string][]byte)
	for _, pr := range prs {
		entryKeyHex := hex.EncodeToString(pr.EntryKey)
		if _, in := entryKeys[entryKeyHex]; !in {
			entryKeys[entryKeyHex] = pr.EntryKey
		}
	}

	entryMetas := make(map[string]*api.EntryMetadata)
	for entryKeyHex, entryKey := range entryKeys { // TODO parallelize
		rq := &courierapi.GetRequest{Key: entryKey}
		ctx, cancel := context.WithTimeout(context.Background(), g.rqTimeout)
		rp, err := g.courier.Get(ctx, rq)
		cancel()
		if err != nil {
			return nil, err
		}
		entry, ok := rp.Value.Contents.(*libriapi.Document_Entry)
		if !ok {
			return nil, ErrDocNotEntry
		}
		entryMetas[entryKeyHex] = &api.EntryMetadata{
			CreatedTime:           entry.Entry.CreatedTime,
			MetadataCiphertext:    entry.Entry.MetadataCiphertext,
			MetadataCiphertextMac: entry.Entry.MetadataCiphertextMac,
		}
	}
	return entryMetas, nil
}

type entitySummaryGetter interface {
	// get returns the entity summaries (keyed by entity ID) for all the reader and author
	// entities in the list of publication receipts.
	get(prs []*catapi.PublicationReceipt) (map[string]*api.EntitySummary, error)
}

type entitySummaryGetterImpl struct {
	rqTimeout time.Duration
	directory directoryapi.DirectoryClient
}

func (g *entitySummaryGetterImpl) get(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntitySummary, error) {

	// get unique set of IDs to avoid unnecessary requests below for repeats
	entityIDs := make(map[string]struct{})
	for _, pr := range prs {
		entityIDs[pr.ReaderEntityId] = struct{}{}
		entityIDs[pr.AuthorEntityId] = struct{}{}
	}

	entitySummaries := make(map[string]*api.EntitySummary)
	for entityID := range entityIDs { // TODO parallelize
		rq := &directoryapi.GetEntityRequest{EntityId: entityID}
		ctx, cancel := context.WithTimeout(context.Background(), g.rqTimeout)
		rp, err := g.directory.GetEntity(ctx, rq)
		cancel()
		if err != nil {
			return nil, err
		}
		entitySummaries[entityID] = &api.EntitySummary{
			EntityId: entityID,
			Type:     rp.Entity.Type(),
			Name:     "", // TODO rp.Entity.Name()
		}
	}
	return entitySummaries, nil
}
