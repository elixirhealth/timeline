package server

import (
	"context"
	"encoding/hex"
	"math/rand"
	"net"
	"testing"
	"time"

	libriapi "github.com/drausin/libri/libri/librarian/api"
	catapi "github.com/elixirhealth/catalog/pkg/catalogapi"
	dirapi "github.com/elixirhealth/directory/pkg/directoryapi"
	bserver "github.com/elixirhealth/service-base/pkg/server"
	"github.com/elixirhealth/service-base/pkg/util"
	api "github.com/elixirhealth/timeline/pkg/timelineapi"
	"github.com/stretchr/testify/assert"
)

var (
	testDummyAddr = &net.TCPAddr{IP: net.ParseIP("localhost"), Port: 20100}
	okConfig      = NewDefaultConfig().
			WithCourierAddr(testDummyAddr).
			WithCatalogAddr(testDummyAddr).
			WithDirectoryAddr(testDummyAddr).
			WithUserAddr(testDummyAddr)
)

func TestNewTimeline_ok(t *testing.T) {
	c, err := newTimeline(okConfig)
	assert.Nil(t, err)
	assert.Equal(t, okConfig, c.config)
	assert.NotEmpty(t, c.entityIDGetter)
	assert.NotEmpty(t, c.pubReceiptGetter)
	assert.NotEmpty(t, c.envelopeGetter)
	assert.NotEmpty(t, c.entryMetadataGetter)
	assert.NotEmpty(t, c.entitySummaryGetter)
}

func TestNewTimeline_err(t *testing.T) {
	badConfigs := map[string]*Config{
		"missing courier config": NewDefaultConfig().
			WithCatalogAddr(testDummyAddr).
			WithDirectoryAddr(testDummyAddr).
			WithUserAddr(testDummyAddr),
		"missing catalog config": NewDefaultConfig().
			WithCourierAddr(testDummyAddr).
			WithDirectoryAddr(testDummyAddr).
			WithUserAddr(testDummyAddr),
		"missing directory config": NewDefaultConfig().
			WithCourierAddr(testDummyAddr).
			WithCatalogAddr(testDummyAddr).
			WithUserAddr(testDummyAddr),
		"missing user config": NewDefaultConfig().
			WithCourierAddr(testDummyAddr).
			WithCatalogAddr(testDummyAddr).
			WithDirectoryAddr(testDummyAddr),
	}
	for desc, badConfig := range badConfigs {
		c, err := newTimeline(badConfig)
		assert.NotNil(t, err, desc)
		assert.Nil(t, c)
	}
}

func TestTimeline_Get_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	nowMicros := time.Now().UnixNano() / 1E3
	entity1 := &dirapi.Entity{
		EntityId: "entity ID 1",
		TypeAttributes: &dirapi.Entity_Patient{
			Patient: &dirapi.Patient{
				LastName: "Last Name 1",
			},
		},
	}
	entity2 := &dirapi.Entity{
		EntityId: "entity ID 2",
		TypeAttributes: &dirapi.Entity_Patient{
			Patient: &dirapi.Patient{
				LastName: "Last Name 2",
			},
		},
	}
	entity3 := &dirapi.Entity{
		EntityId: "entity ID 3",
		TypeAttributes: &dirapi.Entity_Office{
			Office: &dirapi.Office{
				Name: "Office Name",
			},
		},
	}
	entry1, entryDocKey1 := libriapi.NewTestSinglePageEntry(rng), util.RandBytes(rng, 32)
	entry2, entryDocKey2 := libriapi.NewTestSinglePageEntry(rng), util.RandBytes(rng, 32)
	env1 := libriapi.NewTestEnvelope(rng)
	env2 := libriapi.NewTestEnvelope(rng)
	env3 := libriapi.NewTestEnvelope(rng)
	prs := []*catapi.PublicationReceipt{
		{
			ReceivedTime:   nowMicros - 1,
			EntryKey:       entryDocKey1,
			ReaderEntityId: entity1.EntityId,
			AuthorEntityId: entity2.EntityId,
		},
		{
			ReceivedTime:   nowMicros - 2,
			EntryKey:       entryDocKey1,
			ReaderEntityId: entity1.EntityId,
			AuthorEntityId: entity3.EntityId,
		},
		{
			ReceivedTime:   nowMicros - 3,
			EntryKey:       entryDocKey2,
			ReaderEntityId: entity2.EntityId,
			AuthorEntityId: entity3.EntityId,
		},
	}

	tl := &Timeline{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		entityIDGetter: &fixedEntityIDGetter{
			entityIDs: []string{entity1.EntityId, entity2.EntityId},
		},
		pubReceiptGetter: &fixedPubReceiptGetter{
			prs: prs,
		},
		envelopeGetter: &fixedEnvelopeGetter{
			envs: []*libriapi.Envelope{env1, env2, env3},
		},
		entryMetadataGetter: &fixedEntryMetadataGetter{
			em: map[string]*api.EntryMetadata{
				hex.EncodeToString(entryDocKey1): {
					CreatedTime:           entry1.CreatedTime,
					MetadataCiphertext:    entry1.MetadataCiphertext,
					MetadataCiphertextMac: entry1.MetadataCiphertextMac,
				},
				hex.EncodeToString(entryDocKey2): {
					CreatedTime:           entry2.CreatedTime,
					MetadataCiphertext:    entry2.MetadataCiphertext,
					MetadataCiphertextMac: entry2.MetadataCiphertextMac,
				},
			},
		},
		entitySummaryGetter: &fixedEntitySummaryGetter{
			es: map[string]*api.EntitySummary{
				entity1.EntityId: {
					EntityId: entity1.EntityId,
					Type:     entity1.Type(),
				},
				entity2.EntityId: {
					EntityId: entity2.EntityId,
					Type:     entity2.Type(),
				},
				entity3.EntityId: {
					EntityId: entity3.EntityId,
					Type:     entity3.Type(),
				},
			},
		},
	}

	rq := &api.GetRequest{
		UserId: "user ID",
		TimeRange: &api.TimeRange{
			UpperBound: time.Now().UnixNano() / 1E3,
		},
	}
	rp, err := tl.Get(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, len(prs), len(rp.Events))
	for i, event := range rp.Events {
		assert.NotEmpty(t, event.Time)
		if i > 0 {
			// check descending time event ordering
			assert.True(t, event.Time < rp.Events[i-1].Time)
		}
		assert.NotEmpty(t, event.Envelope)
		assert.NotEmpty(t, event.EntryMetadata)
		assert.NotEmpty(t, event.Reader)
		assert.NotEmpty(t, event.Author)
	}
}

func TestTimeline_Get_err(t *testing.T) {
	nowMicros := time.Now().UnixNano() / 1E3
	okRq := &api.GetRequest{
		UserId: "user ID",
		TimeRange: &api.TimeRange{
			UpperBound: nowMicros,
		},
	}
	baseServer := bserver.NewBaseServer(bserver.NewDefaultBaseConfig())
	cases := map[string]struct {
		tl       *Timeline
		rq       *api.GetRequest
		expected error
	}{
		"invalid rq": {
			tl: &Timeline{
				BaseServer: baseServer,
			},
			rq:       &api.GetRequest{},
			expected: api.ErrEmptyUserID,
		},
		"entityIDGetter err": {
			tl: &Timeline{
				BaseServer:     baseServer,
				entityIDGetter: &fixedEntityIDGetter{err: errTest},
			},
			rq:       okRq,
			expected: errTest,
		},
		"pubReceiptGetter err": {
			tl: &Timeline{
				BaseServer:       baseServer,
				entityIDGetter:   &fixedEntityIDGetter{},
				pubReceiptGetter: &fixedPubReceiptGetter{err: errTest},
			},
			rq:       okRq,
			expected: errTest,
		},
		"envelopeGetter err": {
			tl: &Timeline{
				BaseServer:       baseServer,
				entityIDGetter:   &fixedEntityIDGetter{},
				pubReceiptGetter: &fixedPubReceiptGetter{},
				envelopeGetter:   &fixedEnvelopeGetter{err: errTest},
			},
			rq:       okRq,
			expected: errTest,
		},
		"entryMetadataGetter err": {
			tl: &Timeline{
				BaseServer:          baseServer,
				entityIDGetter:      &fixedEntityIDGetter{},
				pubReceiptGetter:    &fixedPubReceiptGetter{},
				envelopeGetter:      &fixedEnvelopeGetter{},
				entryMetadataGetter: &fixedEntryMetadataGetter{err: errTest},
			},
			rq:       okRq,
			expected: errTest,
		},
		"entitySummaryGetter err": {
			tl: &Timeline{
				BaseServer:          baseServer,
				entityIDGetter:      &fixedEntityIDGetter{},
				pubReceiptGetter:    &fixedPubReceiptGetter{},
				envelopeGetter:      &fixedEnvelopeGetter{},
				entryMetadataGetter: &fixedEntryMetadataGetter{},
				entitySummaryGetter: &fixedEntitySummaryGetter{err: errTest},
			},
			rq:       okRq,
			expected: errTest,
		},
	}
	for desc, c := range cases {
		rp, err := c.tl.Get(context.Background(), c.rq)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, rp, desc)
	}
}

type fixedEntityIDGetter struct {
	entityIDs []string
	err       error
}

func (f *fixedEntityIDGetter) get(userID string) ([]string, error) {
	return f.entityIDs, f.err
}

type fixedPubReceiptGetter struct {
	prs []*catapi.PublicationReceipt
	err error
}

func (f *fixedPubReceiptGetter) get(
	entityIDs []string, tr *api.TimeRange, limit uint32,
) ([]*catapi.PublicationReceipt, error) {
	return f.prs, f.err
}

type fixedEnvelopeGetter struct {
	envs []*libriapi.Envelope
	err  error
}

func (f *fixedEnvelopeGetter) get(prs []*catapi.PublicationReceipt) ([]*libriapi.Envelope, error) {
	return f.envs, f.err
}

type fixedEntryMetadataGetter struct {
	em  map[string]*api.EntryMetadata
	err error
}

func (f *fixedEntryMetadataGetter) get(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntryMetadata, error) {
	return f.em, f.err
}

type fixedEntitySummaryGetter struct {
	es  map[string]*api.EntitySummary
	err error
}

func (f *fixedEntitySummaryGetter) get(
	prs []*catapi.PublicationReceipt,
) (map[string]*api.EntitySummary, error) {
	return f.es, f.err
}
