package server

import (
	"encoding/hex"
	"math/rand"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	catapi "github.com/elxirhealth/catalog/pkg/catalogapi"
	"github.com/elxirhealth/courier/pkg/courierapi"
	dirapi "github.com/elxirhealth/directory/pkg/directoryapi"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	"github.com/elxirhealth/user/pkg/userapi"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	errTest = errors.New("some test error")
)

func TestEntityIDGetter(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zapcore.DebugLevel)

	// ok
	entityIDs1 := []string{"entity ID 1", "entity ID 2"}
	g := &entityIDGetterImpl{
		lg: lg,
		user: &fixedUser{
			getRp: &userapi.GetEntitiesResponse{
				EntityIds: entityIDs1,
			},
		},
	}
	entityIDs2, err := g.get("some user ID")
	assert.Nil(t, err)
	assert.Equal(t, entityIDs1, entityIDs2)

	// rq error
	g = &entityIDGetterImpl{
		lg:   lg,
		user: &fixedUser{getErr: errTest},
	}
	entityIDs2, err = g.get("some user ID")
	assert.Equal(t, errTest, err)
	assert.Nil(t, entityIDs2)
}

func TestPubReceiptGetter(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zapcore.DebugLevel)
	now := time.Now().Unix() * 1E6
	envKey1 := append(make([]byte, id.Length-1), 1)
	envKey2 := append(make([]byte, id.Length-1), 2)
	envKey3 := append(make([]byte, id.Length-1), 3)
	envKey4 := append(make([]byte, id.Length-1), 4)
	entryKey1 := append(make([]byte, id.Length-1), 5)
	entryKey2 := append(make([]byte, id.Length-1), 6)
	authorPub1 := append(make([]byte, libriapi.ECPubKeyLength-1), 7)
	authorPub2 := append(make([]byte, libriapi.ECPubKeyLength-1), 8)
	authorEntityID1 := "author entity ID 1"
	authorEntityID2 := "author entity ID 2"
	readerPub1 := append(make([]byte, libriapi.ECPubKeyLength-1), 9)
	readerPub2 := append(make([]byte, libriapi.ECPubKeyLength-1), 10)
	readerPub3 := append(make([]byte, libriapi.ECPubKeyLength-1), 11)
	readerEntityID1 := "reader entity ID 1"
	readerEntityID2 := "reader entity ID 2"

	prs := []*catapi.PublicationReceipt{
		{
			EnvelopeKey:     envKey1,
			EntryKey:        entryKey1,
			AuthorPublicKey: authorPub1,
			AuthorEntityId:  authorEntityID1,
			ReaderPublicKey: readerPub1,
			ReaderEntityId:  readerEntityID1,
			ReceivedTime:    now - 5,
		},
		{
			EnvelopeKey:     envKey2,
			EntryKey:        entryKey1,
			AuthorPublicKey: authorPub1,
			AuthorEntityId:  authorEntityID1,
			ReaderPublicKey: readerPub2,
			ReaderEntityId:  readerEntityID2,
			ReceivedTime:    now - 4,
		},
		{
			EnvelopeKey:     envKey3,
			EntryKey:        entryKey1,
			AuthorPublicKey: authorPub1,
			AuthorEntityId:  authorEntityID1,
			ReaderPublicKey: readerPub3,
			ReaderEntityId:  readerEntityID2,
			ReceivedTime:    now - 3,
		},
		{
			EnvelopeKey:     envKey4,
			EntryKey:        entryKey2,
			AuthorPublicKey: authorPub2,
			AuthorEntityId:  authorEntityID2,
			ReaderPublicKey: readerPub1,
			ReaderEntityId:  readerEntityID1,
			ReceivedTime:    now - 2,
		},
	}

	for parallelism := uint(1); parallelism < DefaultParallelism*2; parallelism++ {

		c := &fixedCatalog{prs: make(map[string][]*catapi.PublicationReceipt)}
		for _, pr := range prs {
			c.prs[pr.ReaderEntityId] = append(c.prs[pr.ReaderEntityId], pr)
		}

		// ok
		g := &pubReceiptGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			catalog:     c,
		}
		tr := &api.TimeRange{UpperBound: now}
		limit := uint32(2)
		gotPRs, err := g.get([]string{readerEntityID1, readerEntityID2}, tr, limit)
		assert.Nil(t, err)
		assert.Equal(t, int(limit), len(gotPRs))
		assert.True(t, gotPRs[0].ReceivedTime > gotPRs[1].ReceivedTime)

		// catalog search err
		g = &pubReceiptGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			catalog:     &fixedCatalog{searchErr: errTest},
		}
		gotPRs, err = g.get([]string{readerEntityID1, readerEntityID2}, tr, limit)
		assert.Equal(t, errTest, err)
		assert.Nil(t, gotPRs)

		// missing author ID err
		g = &pubReceiptGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			catalog: &fixedCatalog{
				prs: map[string][]*catapi.PublicationReceipt{
					readerEntityID1: {
						{
							EnvelopeKey:     envKey1,
							EntryKey:        entryKey1,
							AuthorPublicKey: authorPub1,
							AuthorEntityId:  "", // missing
							ReaderPublicKey: readerPub1,
							ReaderEntityId:  readerEntityID1,
							ReceivedTime:    now - 5,
						},
					},
				},
			},
		}
		gotPRs, err = g.get([]string{readerEntityID1, readerEntityID2}, tr, limit)
		assert.Equal(t, ErrMissingAuthorEntityID, err)
		assert.Nil(t, gotPRs)
	}
}

func TestEnvelopeGetter(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zapcore.DebugLevel)
	rng := rand.New(rand.NewSource(0))
	env1 := libriapi.NewTestEnvelope(rng)
	envDoc1 := &libriapi.Document{
		Contents: &libriapi.Document_Envelope{
			Envelope: env1,
		},
	}
	envKey1, err := libriapi.GetKey(envDoc1)
	assert.Nil(t, err)
	env2 := libriapi.NewTestEnvelope(rng)
	envDoc2 := &libriapi.Document{
		Contents: &libriapi.Document_Envelope{
			Envelope: env2,
		},
	}
	envKey2, err := libriapi.GetKey(envDoc2)
	assert.Nil(t, err)

	for parallelism := uint(1); parallelism < DefaultParallelism*2; parallelism++ {

		prs := []*catapi.PublicationReceipt{
			// other fields would normally be populated but aren't needed for test
			{EnvelopeKey: envKey1.Bytes()},
			{EnvelopeKey: envKey2.Bytes()},
		}

		// ok
		c := &fixedCourier{
			docs: map[string]*libriapi.Document{
				envKey1.String(): envDoc1,
				envKey2.String(): envDoc2,
			},
		}
		g := &envelopeGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			courier:     c,
		}
		envs, err := g.get(prs)
		assert.Nil(t, err)
		assert.Equal(t, []*libriapi.Envelope{env1, env2}, envs)

		// courier get err
		g = &envelopeGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			courier:     &fixedCourier{getErr: errTest},
		}
		envs, err = g.get(prs)
		assert.Equal(t, errTest, err)
		assert.Nil(t, envs)

		// doc not envelope err
		entryDoc, entryDocKey := libriapi.NewTestDocument(rng)
		c = &fixedCourier{
			docs: map[string]*libriapi.Document{
				entryDocKey.String(): entryDoc,
			},
		}
		g = &envelopeGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			courier:     c,
		}
		prs = []*catapi.PublicationReceipt{
			// other fields would normally be populated but aren't needed for test
			{EnvelopeKey: entryDocKey.Bytes()},
		}
		envs, err = g.get(prs)
		assert.Equal(t, ErrDocNotEnvelope, err)
		assert.Nil(t, envs)
	}
}

func TestEntryMetadataGetter(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zapcore.DebugLevel)
	rng := rand.New(rand.NewSource(0))
	entryDoc1, entryDocKey1 := libriapi.NewTestDocument(rng)
	entryDoc2, entryDocKey2 := libriapi.NewTestDocument(rng)

	for parallelism := uint(1); parallelism < DefaultParallelism*2; parallelism++ {

		// ok
		c := &fixedCourier{
			docs: map[string]*libriapi.Document{
				entryDocKey1.String(): entryDoc1,
				entryDocKey2.String(): entryDoc2,
			},
		}
		g := &entryMetadataGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			courier:     c,
		}
		prs := []*catapi.PublicationReceipt{
			// other fields would normally be populated but aren't needed for test
			{EntryKey: entryDocKey1.Bytes()},
			{EntryKey: entryDocKey2.Bytes()},
			{EntryKey: entryDocKey1.Bytes()}, // repeat, to test dedup
		}
		em, err := g.get(prs)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(em))
		assert.Contains(t, em, entryDocKey1.String())
		assert.Contains(t, em, entryDocKey2.String())

		// courier get err
		g = &entryMetadataGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			courier:     &fixedCourier{getErr: errTest},
		}
		em, err = g.get(prs)
		assert.Equal(t, errTest, err)
		assert.Nil(t, em)

		// doc not entry err
		envDoc := &libriapi.Document{
			Contents: &libriapi.Document_Envelope{
				Envelope: libriapi.NewTestEnvelope(rng),
			},
		}
		envDocKey, err := libriapi.GetKey(envDoc)
		c = &fixedCourier{
			docs: map[string]*libriapi.Document{
				envDocKey.String(): envDoc,
			},
		}
		g = &entryMetadataGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			courier:     c,
		}
		prs = []*catapi.PublicationReceipt{
			// other fields would normally be populated but aren't needed for test
			{EntryKey: envDocKey.Bytes()},
		}
		em, err = g.get(prs)
		assert.Equal(t, ErrDocNotEntry, err)
		assert.Nil(t, em)

	}
}

func TestEntitySummaryGetter(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zapcore.DebugLevel)

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

	for parallelism := uint(1); parallelism < DefaultParallelism*2; parallelism++ {

		// ok
		d := &fixedDirectory{
			entities: map[string]*dirapi.Entity{
				entity1.EntityId: entity1,
				entity2.EntityId: entity2,
				entity3.EntityId: entity3,
			},
		}
		g := &entitySummaryGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			directory:   d,
		}
		prs := []*catapi.PublicationReceipt{
			{
				ReaderEntityId: entity1.EntityId,
				AuthorEntityId: entity2.EntityId,
			},
			{
				ReaderEntityId: entity1.EntityId,
				AuthorEntityId: entity3.EntityId,
			},
			{
				ReaderEntityId: entity2.EntityId,
				AuthorEntityId: entity3.EntityId,
			},
		}
		es, err := g.get(prs)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(es))
		for entityID := range d.entities {
			assert.Equal(t, entityID, es[entityID].EntityId)
			assert.NotEmpty(t, es[entityID].Type)
		}

		// directory get entity err
		g = &entitySummaryGetterImpl{
			lg:          lg,
			parallelism: parallelism,
			directory: &fixedDirectory{
				getEntityErr: errTest,
			},
		}
		es, err = g.get(prs)
		assert.Equal(t, errTest, err)
		assert.Nil(t, es)

	}
}

type fixedUser struct {
	getRp  *userapi.GetEntitiesResponse
	getErr error
}

func (f *fixedUser) AddEntity(
	ctx context.Context, in *userapi.AddEntityRequest, opts ...grpc.CallOption,
) (*userapi.AddEntityResponse, error) {
	panic("implement me")
}

func (f *fixedUser) GetEntities(
	ctx context.Context, in *userapi.GetEntitiesRequest, opts ...grpc.CallOption,
) (*userapi.GetEntitiesResponse, error) {
	return f.getRp, f.getErr
}

type fixedCatalog struct {
	// key is reader entity ID for easy searching
	prs       map[string][]*catapi.PublicationReceipt
	searchErr error
}

func (f *fixedCatalog) Put(
	ctx context.Context, rq *catapi.PutRequest, opts ...grpc.CallOption,
) (*catapi.PutResponse, error) {
	panic("implement me")
}

func (f *fixedCatalog) Search(
	ctx context.Context, rq *catapi.SearchRequest, opts ...grpc.CallOption,
) (*catapi.SearchResponse, error) {
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	prs, in := f.prs[rq.ReaderEntityId]
	if !in {
		return &catapi.SearchResponse{
			Result: make([]*catapi.PublicationReceipt, 0),
		}, nil
	}
	return &catapi.SearchResponse{Result: prs}, nil
}

type fixedCourier struct {
	docs   map[string]*libriapi.Document
	getErr error
}

func (f *fixedCourier) Put(
	ctx context.Context, in *courierapi.PutRequest, opts ...grpc.CallOption,
) (*courierapi.PutResponse, error) {
	panic("implement me")
}

func (f *fixedCourier) Get(
	ctx context.Context, in *courierapi.GetRequest, opts ...grpc.CallOption,
) (*courierapi.GetResponse, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	keyHex := hex.EncodeToString(in.Key)
	return &courierapi.GetResponse{Value: f.docs[keyHex]}, nil
}

type fixedDirectory struct {
	entities     map[string]*dirapi.Entity
	getEntityErr error
}

func (f *fixedDirectory) PutEntity(
	ctx context.Context, in *dirapi.PutEntityRequest, opts ...grpc.CallOption,
) (*dirapi.PutEntityResponse, error) {
	panic("implement me")
}

func (f *fixedDirectory) GetEntity(
	ctx context.Context, rq *dirapi.GetEntityRequest, opts ...grpc.CallOption,
) (*dirapi.GetEntityResponse, error) {
	if f.getEntityErr != nil {
		return nil, f.getEntityErr
	}
	return &dirapi.GetEntityResponse{Entity: f.entities[rq.EntityId]}, nil
}

func (f *fixedDirectory) SearchEntity(
	ctx context.Context, in *dirapi.SearchEntityRequest, opts ...grpc.CallOption,
) (*dirapi.SearchEntityResponse, error) {
	panic("implement me")
}
