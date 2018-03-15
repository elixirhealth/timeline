package server

import (
	"container/heap"
	"encoding/hex"
	"sync"
	"time"

	libriapi "github.com/drausin/libri/libri/librarian/api"
	catapi "github.com/elxirhealth/catalog/pkg/catalogapi"
	"github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/elxirhealth/directory/pkg/directoryapi"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	"github.com/elxirhealth/user/pkg/userapi"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type entityIDGetter interface {
	get(userID string) ([]string, error)
}

type entityIDGetterImpl struct {
	lg        *zap.Logger
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
	g.lg.Debug("got entities for user", logEntityIDGet(userID, rp.EntityIds)...)
	return rp.EntityIds, nil
}

type pubReceiptGetter interface {
	get(
		entityIDs []string, tr *api.TimeRange, limit uint32,
	) ([]*catapi.PublicationReceipt, error)
}

type pubReceiptGetterImpl struct {
	lg          *zap.Logger
	parallelism uint
	rqTimeout   time.Duration
	catalog     catapi.CatalogClient
}

func (g *pubReceiptGetterImpl) get(
	entityIDs []string, tr *api.TimeRange, limit uint32,
) ([]*catapi.PublicationReceipt, error) {

	mu := new(sync.Mutex)
	entityIDCh := toLoadedStringChan(entityIDs)
	errs := make(chan error, g.parallelism)
	errored := safeFlag{}

	prHeap := &publicationReceipts{}
	wg1 := new(sync.WaitGroup)
	for i := uint(0); i < g.parallelism; i++ {
		wg1.Add(1)
		go func(wg2 *sync.WaitGroup) {
			defer wg2.Done()
			for rEntityID := range entityIDCh {
				if errored.isTrue() {
					return
				}
				rq := &catapi.SearchRequest{
					ReaderEntityId: rEntityID,
					AuthorEntityId: "",
					After:          tr.LowerBound,
					Before:         tr.UpperBound,
					Limit:          limit,
				}

				ctx, cancel := context.WithTimeout(bgCtx(), g.rqTimeout)
				rp, err := g.catalog.Search(ctx, rq)
				cancel()
				if err != nil {
					errs <- err
					errored.setTrue()
					return
				}
				for _, readerPR := range rp.Result {
					if readerPR.AuthorEntityId == "" {
						errs <- ErrMissingAuthorEntityID
						errored.setTrue()
						return
					}
					mu.Lock()
					heap.Push(prHeap, readerPR)
					if prHeap.Len() > int(limit) {
						heap.Pop(prHeap)
					}
					mu.Unlock()
				}
				g.lg.Debug("got publications for entity",
					logPubsGet(rEntityID, rp.Result)...)
				if errored.isTrue() {
					return
				}
			}
		}(wg1)
	}
	wg1.Wait()

	select {
	case err := <-errs:
		return nil, err
	default:
	}

	// create list of PRs, sorted descending by received time
	prs := make([]*catapi.PublicationReceipt, prHeap.Len())
	for i := len(prs) - 1; i >= 0; i-- {
		prs[i] = heap.Pop(prHeap).(*catapi.PublicationReceipt)
	}
	g.lg.Debug("got all publications for entities", logAllPubsGet(entityIDs, prs)...)
	return prs, nil
}

type envelopeGetter interface {
	get(prs []*catapi.PublicationReceipt) ([]*libriapi.Envelope, error)
}

type envelopeGetterImpl struct {
	lg          *zap.Logger
	parallelism uint
	rqTimeout   time.Duration
	courier     courierapi.CourierClient
}

func (g *envelopeGetterImpl) get(prs []*catapi.PublicationReceipt) ([]*libriapi.Envelope, error) {
	envs := make([]*libriapi.Envelope, len(prs))

	mu := new(sync.Mutex)
	prsCh := toLoadedPRChan(prs)
	errs := make(chan error, g.parallelism)
	errored := safeFlag{}

	envKeyIdxs := make(map[string]int)
	for i, pr := range prs {
		envKeyIdxs[hex.EncodeToString(pr.EnvelopeKey)] = i
	}

	wg1 := new(sync.WaitGroup)
	for c := uint(0); c < g.parallelism; c++ {
		wg1.Add(1)
		go func(wg2 *sync.WaitGroup) {
			defer wg2.Done()
			for pr := range prsCh {
				if errored.isTrue() {
					return
				}
				rq := &courierapi.GetRequest{Key: pr.EnvelopeKey}
				ctx, cancel := context.WithTimeout(bgCtx(), g.rqTimeout)
				rp, err := g.courier.Get(ctx, rq)
				cancel()
				if err != nil {
					errs <- err
					errored.setTrue()
					return
				}
				env, ok := rp.Value.Contents.(*libriapi.Document_Envelope)
				if !ok {
					errs <- ErrDocNotEnvelope
					errored.setTrue()
					return
				}
				i := envKeyIdxs[hex.EncodeToString(pr.EnvelopeKey)]
				mu.Lock()
				envs[i] = env.Envelope
				mu.Unlock()
				g.lg.Debug("got envelope",
					zap.String(logEnvKeyShort, shortHex(pr.EnvelopeKey)))
				if errored.isTrue() {
					return
				}
			}
		}(wg1)
	}
	wg1.Wait()

	select {
	case err := <-errs:
		return nil, err
	default:
		g.lg.Debug("got all envelopes", zap.Int(logNEnvelopes, len(envs)))
		return envs, nil
	}
}

type entryMetadataGetter interface {
	// get returns the (encrypted) entry metadata (keyed by entry key hex) for all entries in
	// the list of publication receipts.
	get(prs []*catapi.PublicationReceipt) (map[string]*api.EntryMetadata, error)
}

type entryMetadataGetterImpl struct {
	lg          *zap.Logger
	parallelism uint
	rqTimeout   time.Duration
	courier     courierapi.CourierClient
}

func (g *entryMetadataGetterImpl) get(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntryMetadata, error) {

	// get unique set of keys to avoid unnecessary requests below for repeats
	entryKeys := make(map[string][]byte)
	entryKeyHexsCh := make(chan string, len(prs))
	for _, pr := range prs {
		entryKeyHex := hex.EncodeToString(pr.EntryKey)
		if _, in := entryKeys[entryKeyHex]; !in {
			entryKeys[entryKeyHex] = pr.EntryKey
			entryKeyHexsCh <- entryKeyHex
		}
	}
	close(entryKeyHexsCh)

	mu := new(sync.Mutex)
	errs := make(chan error, g.parallelism)
	errored := safeFlag{}
	entryMetas := make(map[string]*api.EntryMetadata)

	wg1 := new(sync.WaitGroup)
	for c := uint(0); c < g.parallelism; c++ {
		wg1.Add(1)
		go func(wg2 *sync.WaitGroup) {
			defer wg2.Done()
			for entryKeyHex := range entryKeyHexsCh {
				if errored.isTrue() {
					return
				}
				entryKey := entryKeys[entryKeyHex]
				rq := &courierapi.GetRequest{Key: entryKey}
				ctx, cancel := context.WithTimeout(bgCtx(), g.rqTimeout)
				rp, err := g.courier.Get(ctx, rq)
				cancel()
				if err != nil {
					errs <- err
					errored.setTrue()
					return
				}
				entry, ok := rp.Value.Contents.(*libriapi.Document_Entry)
				if !ok {
					errs <- ErrDocNotEntry
					errored.setTrue()
					return
				}
				mu.Lock()
				entryMetas[entryKeyHex] = &api.EntryMetadata{
					CreatedTime:           entry.Entry.CreatedTime,
					MetadataCiphertext:    entry.Entry.MetadataCiphertext,
					MetadataCiphertextMac: entry.Entry.MetadataCiphertextMac,
				}
				mu.Unlock()
				g.lg.Debug("got entry metadata",
					zap.String(logEntryKeyShort, shortHex(entryKey)))
				if errored.isTrue() {
					return
				}
			}
		}(wg1)
	}
	wg1.Wait()

	select {
	case err := <-errs:
		return nil, err
	default:
		g.lg.Debug("got all entry metadata", zap.Int(logNEntries, len(entryMetas)))
		return entryMetas, nil
	}
}

type entitySummaryGetter interface {
	// get returns the entity summaries (keyed by entity ID) for all the reader and author
	// entities in the list of publication receipts.
	get(prs []*catapi.PublicationReceipt) (map[string]*api.EntitySummary, error)
}

type entitySummaryGetterImpl struct {
	lg          *zap.Logger
	parallelism uint
	rqTimeout   time.Duration
	directory   directoryapi.DirectoryClient
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
	entityIDsCh := make(chan string, len(entityIDs))
	for entityID := range entityIDs {
		entityIDsCh <- entityID
	}
	close(entityIDsCh)

	mu := new(sync.Mutex)
	errs := make(chan error, g.parallelism)
	errored := safeFlag{}
	entitySummaries := make(map[string]*api.EntitySummary)

	wg1 := new(sync.WaitGroup)
	for c := uint(0); c < g.parallelism; c++ {
		wg1.Add(1)
		go func(wg2 *sync.WaitGroup) {
			defer wg2.Done()
			for entityID := range entityIDs {
				rq := &directoryapi.GetEntityRequest{EntityId: entityID}
				ctx, cancel := context.WithTimeout(bgCtx(), g.rqTimeout)
				rp, err := g.directory.GetEntity(ctx, rq)
				cancel()
				if err != nil {
					errs <- err
					errored.setTrue()
					return
				}
				mu.Lock()
				entitySummaries[entityID] = &api.EntitySummary{
					EntityId: entityID,
					Type:     rp.Entity.Type(),
					Name:     "", // TODO rp.Entity.Name()
				}
				mu.Unlock()
				g.lg.Debug("got entity summary", zap.String(logEntityID, entityID))
			}
		}(wg1)
	}
	wg1.Wait()

	select {
	case err := <-errs:
		return nil, err
	default:
		g.lg.Debug("got all entity summaries", zap.Int(logNEntities, len(entitySummaries)))
		return entitySummaries, nil
	}
}

func toLoadedStringChan(vals []string) chan string {
	ch := make(chan string, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func toLoadedPRChan(prs []*catapi.PublicationReceipt) chan *catapi.PublicationReceipt {
	ch := make(chan *catapi.PublicationReceipt, len(prs))
	for _, pr := range prs {
		ch <- pr
	}
	close(ch)
	return ch
}

type safeFlag struct {
	val bool
	mu  sync.Mutex
}

func (sf safeFlag) setTrue() {
	sf.mu.Lock()
	sf.val = true
	sf.mu.Unlock()
}

func (sf safeFlag) isTrue() bool {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.val
}

func bgCtx() context.Context {
	return context.Background()
}
