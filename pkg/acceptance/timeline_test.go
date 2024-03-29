// +build acceptance

package acceptance

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elixirhealth/catalog/pkg/catalogapi"
	catclient "github.com/elixirhealth/catalog/pkg/client"
	catserver "github.com/elixirhealth/catalog/pkg/server"
	catstorage "github.com/elixirhealth/catalog/pkg/server/storage"
	"github.com/elixirhealth/courier/pkg/cache"
	cclient "github.com/elixirhealth/courier/pkg/client"
	"github.com/elixirhealth/courier/pkg/courierapi"
	cserver "github.com/elixirhealth/courier/pkg/server"
	dirclient "github.com/elixirhealth/directory/pkg/client"
	dirapi "github.com/elixirhealth/directory/pkg/directoryapi"
	dirserver "github.com/elixirhealth/directory/pkg/server"
	"github.com/elixirhealth/directory/pkg/server/storage"
	"github.com/elixirhealth/directory/pkg/server/storage/postgres/migrations"
	keyclient "github.com/elixirhealth/key/pkg/client"
	"github.com/elixirhealth/key/pkg/keyapi"
	keystorage "github.com/elixirhealth/key/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/elixirhealth/timeline/pkg/client"
	"github.com/elixirhealth/timeline/pkg/server"
	api "github.com/elixirhealth/timeline/pkg/timelineapi"
	userclient "github.com/elixirhealth/user/pkg/client"
	userserver "github.com/elixirhealth/user/pkg/server"
	userstorage "github.com/elixirhealth/user/pkg/server/storage"
	"github.com/elixirhealth/user/pkg/userapi"
	"github.com/mattes/migrate/source/go-bindata"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"

	keyserver "github.com/elixirhealth/key/pkg/server"
)

const (
	datastoreEmulatorHostEnv = "DATASTORE_EMULATOR_HOST"
)

type parameters struct {
	nTimelines int

	nUsers        int
	nUserEntities int
	nEntityKeys   int
	nEntryDocs    int
	nMaxShares    int

	rqTimeout     time.Duration
	gcpProjectID  string
	datastoreAddr string

	timelineLogLevel  zapcore.Level
	catalogLogLevel   zapcore.Level
	courierLogLevel   zapcore.Level
	directoryLogLevel zapcore.Level
	keyLogLevel       zapcore.Level
	userLogLevel      zapcore.Level
}

func (p *parameters) getCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), p.rqTimeout)
}

type state struct {
	timelines []*server.Timeline
	courier   *cserver.Courier
	catalog   *catserver.Catalog
	directory *dirserver.Directory
	key       *keyserver.Key
	user      *userserver.User

	timelineClients []api.TimelineClient
	directoryClient dirapi.DirectoryClient
	courierClient   courierapi.CourierClient
	catalogClient   catalogapi.CatalogClient
	keyClient       keyapi.KeyClient
	userClient      userapi.UserClient

	courierAddr   *net.TCPAddr
	catalogAddr   *net.TCPAddr
	directoryAddr *net.TCPAddr
	keyAddr       *net.TCPAddr
	userAddr      *net.TCPAddr

	userEntityIDs       [][]string
	entityReaderPubKeys map[string][][]byte
	entityAuthorPubKeys map[string][][]byte
	userEntries         map[string]map[string]struct{}

	rng               *rand.Rand
	dataDir           string
	events            map[string][]*api.Event
	dbURL             string
	datastoreEmulator *os.Process
	tearDownPostgres  func() error
}

func TestAcceptance(t *testing.T) {
	params := &parameters{
		nTimelines:    3,
		nUsers:        8,
		nUserEntities: 2,
		nEntityKeys:   32,
		nEntryDocs:    64,
		nMaxShares:    2,

		rqTimeout:         10 * time.Second, // very large b/c there are lots of services
		datastoreAddr:     "localhost:2001",
		gcpProjectID:      "dummy-acceptance-id",
		timelineLogLevel:  zapcore.InfoLevel,
		catalogLogLevel:   zapcore.InfoLevel,
		courierLogLevel:   zapcore.InfoLevel,
		directoryLogLevel: zapcore.InfoLevel,
		keyLogLevel:       zapcore.InfoLevel,
		userLogLevel:      zapcore.InfoLevel,
	}
	st := setUp(params)

	createEvents(t, params, st)

	testGetTimeline(t, params, st)

	tearDown(t, st)
}

func testGetTimeline(t *testing.T, params *parameters, st *state) {
	for i := 0; i < params.nUsers; i++ {
		userID := getUserID(i)
		tlClient := st.timelineClients[st.rng.Intn(len(st.timelineClients))]

		ctx, cancel := params.getCtx()
		rp, err := tlClient.Get(ctx, &api.GetRequest{
			UserId:    getUserID(i),
			TimeRange: &api.TimeRange{},
		})
		cancel()
		assert.Nil(t, err)
		if rp == nil {
			// avoid nil pointer panics below
			continue
		}
		assert.True(t, len(rp.Events) > 0)

		for i, ev := range rp.Events {
			if i > 0 {
				assert.True(t, rp.Events[i].Time <= rp.Events[i-1].Time)
			}
			entryKeyHex := hex.EncodeToString(ev.Envelope.EntryKey)
			_, in := st.userEntries[userID][entryKeyHex]
			assert.True(t, in)
		}
		assert.Equal(t, len(st.userEntries[userID]), len(rp.Events))
	}
}

func createEvents(t *testing.T, params *parameters, st *state) {
	st.userEntityIDs = make([][]string, params.nUsers)
	st.userEntries = make(map[string]map[string]struct{})
	st.entityAuthorPubKeys = make(map[string][][]byte)
	st.entityReaderPubKeys = make(map[string][][]byte)

	for i := 0; i < params.nUsers; i++ {
		userID := getUserID(i)
		st.userEntityIDs[i] = make([]string, params.nUserEntities)
		st.userEntries[userID] = make(map[string]struct{})
		for j := 0; j < params.nUserEntities; j++ {

			// add entity
			ctx, cancel := params.getCtx()
			rp, err := st.directory.PutEntity(ctx, &dirapi.PutEntityRequest{
				Entity: dirapi.NewPatient("", &dirapi.Patient{
					LastName:   fmt.Sprintf("Last Name %d", i),
					FirstName:  fmt.Sprintf("First Name %d", i),
					MiddleName: fmt.Sprintf("Middle Name %d", i),
					Birthdate: &dirapi.Date{
						Year:  2006,
						Month: 1 + uint32(i/28),
						Day:   1 + uint32(i%28),
					},
				}),
			})
			cancel()
			assert.Nil(t, err)
			entityID := rp.EntityId
			st.userEntityIDs[i][j] = entityID

			// add entity to user
			ctx, cancel = params.getCtx()
			op := func() error {
				_, err = st.userClient.AddEntity(ctx, &userapi.AddEntityRequest{
					UserId:   userID,
					EntityId: rp.EntityId,
				})
				return err
			}
			err = backoff.Retry(op, newTimeoutExpBackoff(params.rqTimeout))
			cancel()
			assert.Nil(t, err)

			// create author and reader keys for entity
			authorKeys := make([][]byte, params.nEntityKeys)
			readerKeys := make([][]byte, params.nEntityKeys)
			for i := range authorKeys {
				authorKeys[i] = util.RandBytes(st.rng, 33)
				readerKeys[i] = util.RandBytes(st.rng, 33)
			}
			st.entityAuthorPubKeys[entityID] = authorKeys
			st.entityReaderPubKeys[entityID] = readerKeys

			ctx, cancel = params.getCtx()
			_, err = st.keyClient.AddPublicKeys(ctx, &keyapi.AddPublicKeysRequest{
				EntityId:   entityID,
				KeyType:    keyapi.KeyType_AUTHOR,
				PublicKeys: authorKeys,
			})
			cancel()
			assert.Nil(t, err)

			ctx, cancel = params.getCtx()
			_, err = st.keyClient.AddPublicKeys(ctx, &keyapi.AddPublicKeysRequest{
				EntityId:   entityID,
				KeyType:    keyapi.KeyType_READER,
				PublicKeys: readerKeys,
			})
			cancel()
			assert.Nil(t, err)
		}
	}

	// create entries and share them with other entities
	for c := 0; c < params.nEntryDocs; c++ {
		entityID, userID := getRandEntityID(st, params)
		authorPubKey := getRandPubKey(st, params, entityID, keyapi.KeyType_AUTHOR)
		readerPubKey := getRandPubKey(st, params, entityID, keyapi.KeyType_READER)

		// put entry
		entryDocKey, err := putEntry(st, params, authorPubKey)
		assert.Nil(t, err)
		entryDocKeyHex := hex.EncodeToString(entryDocKey)

		// share with self
		err = shareEnv(st, params, entryDocKey, authorPubKey, readerPubKey)
		assert.Nil(t, err)
		st.userEntries[userID][entryDocKeyHex] = struct{}{}

		// share with some others
		nShares := st.rng.Intn(params.nMaxShares)
		for d := 0; d < nShares; d++ {
			readerUserID := userID
			var readerEntityID string
			for readerUserID == userID {
				// want user shared with to be diff than author
				readerEntityID, readerUserID = getRandEntityID(st, params)
			}

			readerPubKey = getRandPubKey(st, params, readerEntityID,
				keyapi.KeyType_READER)
			err = shareEnv(st, params, entryDocKey, authorPubKey, readerPubKey)
			assert.Nil(t, err)
			st.userEntries[readerUserID][entryDocKeyHex] = struct{}{}
		}
	}
}

func putEntry(st *state, params *parameters, authorPubKey []byte) ([]byte, error) {
	entryDoc := &libriapi.Document{
		Contents: &libriapi.Document_Entry{
			Entry: &libriapi.Entry{
				AuthorPublicKey: authorPubKey,
				Page: &libriapi.Page{
					AuthorPublicKey: authorPubKey,
					Ciphertext:      util.RandBytes(st.rng, 128),
					CiphertextMac:   util.RandBytes(st.rng, 32),
				},
				CreatedTime:           uint32(time.Now().Unix()),
				MetadataCiphertext:    util.RandBytes(st.rng, 128),
				MetadataCiphertextMac: util.RandBytes(st.rng, 32),
			},
		},
	}
	entryDocKey, err := libriapi.GetKey(entryDoc)
	errors.MaybePanic(err)
	ctx, cancel := params.getCtx()
	defer cancel()
	_, err = st.courierClient.Put(ctx, &courierapi.PutRequest{
		Key:   entryDocKey.Bytes(),
		Value: entryDoc,
	})
	return entryDocKey.Bytes(), err
}

func shareEnv(st *state, params *parameters, entryKey, authorPubKey, readerPubKey []byte) error {
	env := &libriapi.Envelope{
		EntryKey:         entryKey,
		AuthorPublicKey:  authorPubKey,
		ReaderPublicKey:  readerPubKey,
		EekCiphertext:    util.RandBytes(st.rng, libriapi.EEKCiphertextLength),
		EekCiphertextMac: util.RandBytes(st.rng, libriapi.HMAC256Length),
	}
	envDoc := &libriapi.Document{
		Contents: &libriapi.Document_Envelope{
			Envelope: env,
		},
	}
	envDocKey, err := libriapi.GetKey(envDoc)
	errors.MaybePanic(err)
	ctx, cancel := params.getCtx()
	defer cancel()
	_, err = st.courierClient.Put(ctx, &courierapi.PutRequest{
		Key:   envDocKey.Bytes(),
		Value: envDoc,
	})
	return err
}

func getRandEntityID(st *state, params *parameters) (string, string) {
	userIdx := st.rng.Intn(params.nUsers)
	entityIdx := st.rng.Intn(params.nUserEntities)
	return st.userEntityIDs[userIdx][entityIdx], getUserID(userIdx)
}

func getRandPubKey(st *state, params *parameters, entityID string, kt keyapi.KeyType) []byte {
	pubKeyIdx := st.rng.Intn(params.nEntityKeys)
	if kt == keyapi.KeyType_AUTHOR {
		return st.entityAuthorPubKeys[entityID][pubKeyIdx]
	}
	return st.entityReaderPubKeys[entityID][pubKeyIdx]
}

func getUserID(userIdx int) string {
	return fmt.Sprintf("User-%d", userIdx)
}

func newTimeoutExpBackoff(timeout time.Duration) backoff.BackOff {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = timeout
	return bo
}

func setUp(params *parameters) *state {
	dbURL, cleanup, err := bstorage.StartTestPostgres()
	errors.MaybePanic(err)
	st := &state{
		rng:              rand.New(rand.NewSource(0)),
		dbURL:            dbURL,
		tearDownPostgres: cleanup,
	}
	startDatastoreEmulator(params, st)

	createAndStartCatalog(params, st)
	createAndStartKey(params, st)
	createAndStartCourier(params, st)
	createAndStartDirectory(params, st)
	createAndStartUser(params, st)

	createAndStartTimelines(params, st)

	return st
}

func tearDown(t *testing.T, st *state) {
	// stop datastore emulator
	pgid, err := syscall.Getpgid(st.datastoreEmulator.Pid)
	errors.MaybePanic(err)
	err = syscall.Kill(-pgid, syscall.SIGKILL)
	errors.MaybePanic(err)

	logger := &bstorage.ZapLogger{Logger: logging.NewDevInfoLogger()}
	m := bstorage.NewBindataMigrator(
		st.dbURL,
		bindata.Resource(migrations.AssetNames(), migrations.Asset),
		logger,
	)
	err = m.Down()
	assert.Nil(t, err)

	err = st.tearDownPostgres()
	assert.Nil(t, err)
}

func startDatastoreEmulator(params *parameters, st *state) {
	datastoreDataDir := path.Join(st.dataDir, "datastore")
	cmd := exec.Command("gcloud", "beta", "emulators", "datastore", "start",
		"--no-store-on-disk",
		"--host-port", params.datastoreAddr,
		"--project", params.gcpProjectID,
		"--data-dir", datastoreDataDir,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	errors.MaybePanic(err)
	st.datastoreEmulator = cmd.Process
	os.Setenv(datastoreEmulatorHostEnv, params.datastoreAddr)
}

func createAndStartTimelines(params *parameters, st *state) {
	configs, addrs := newTimelineConfigs(st, params)
	timelines := make([]*server.Timeline, params.nTimelines)
	timelineClients := make([]api.TimelineClient, params.nTimelines)
	up := make(chan *server.Timeline, 1)

	for i := 0; i < params.nTimelines; i++ {
		go func() {
			err := server.Start(configs[i], up)
			errors.MaybePanic(err)
		}()

		// wait for timeline to come up
		timelines[i] = <-up

		// set up timeline client for it
		cl, err := client.NewInsecure(addrs[i].String())
		errors.MaybePanic(err)
		timelineClients[i] = cl
	}

	st.timelines = timelines
	st.timelineClients = timelineClients
}

func newTimelineConfigs(st *state, params *parameters) ([]*server.Config, []*net.TCPAddr) {
	startPort := 10000
	configs := make([]*server.Config, params.nTimelines)
	addrs := make([]*net.TCPAddr, params.nTimelines)

	for i := 0; i < params.nTimelines; i++ {
		serverPort, metricsPort := startPort+i*10, startPort+i*10+1
		configs[i] = server.NewDefaultConfig().
			WithCourierAddr(st.courierAddr).
			WithCatalogAddr(st.catalogAddr).
			WithDirectoryAddr(st.directoryAddr).
			WithUserAddr(st.userAddr)
		configs[i].WithServerPort(uint(serverPort)).
			WithMetricsPort(uint(metricsPort)).
			WithLogLevel(params.timelineLogLevel)

		// seems that DataStore emulator doesn't handle parallel searches well
		configs[i].Parallelism = 1
		addrs[i] = &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPort}
	}
	return configs, addrs
}

func createAndStartCourier(params *parameters, st *state) {
	config, addr := newCourierConfig(st, params)
	up := make(chan *cserver.Courier, 1)

	go func() {
		err := cserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for courier to come up
	courier := <-up

	courierClient, err := cclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.courier = courier
	st.courierAddr = addr
	st.courierClient = courierClient
}

func newCourierConfig(st *state, params *parameters) (*cserver.Config, *net.TCPAddr) {
	serverPort := 10100

	// set eviction params to ensure that evictions actually happen during test
	cacheParams := cache.NewDefaultParameters()
	cacheParams.Type = bstorage.DataStore
	cacheParams.LRUCacheSize = 4
	cacheParams.EvictionBatchSize = 4
	cacheParams.EvictionQueryTimeout = 5 * time.Second
	cacheParams.RecentWindowDays = -1 // i.e., everything is evictable
	cacheParams.EvictionPeriod = 5 * time.Second

	dummyLibAddrs := []*net.TCPAddr{{IP: net.ParseIP("localhost"), Port: 20100}}
	config := cserver.NewDefaultConfig().
		WithLibrarianAddrs(dummyLibAddrs).
		WithCatalogAddr(st.catalogAddr).
		WithKeyAddr(st.keyAddr).
		WithCache(cacheParams).
		WithGCPProjectID(params.gcpProjectID)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(serverPort + 1)).
		WithLogLevel(params.courierLogLevel)

	// since no Libri
	config.NLibriPutters = 0
	config.SubscribeTo.NSubscriptions = 0
	config.LibriPutQueueSize = uint(params.nEntryDocs*(1+params.nMaxShares) + 1)

	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPort}
	return config, addr
}

func createAndStartCatalog(params *parameters, st *state) {
	config, addr := newCatalogConfig(params)
	up := make(chan *catserver.Catalog, 1)

	go func() {
		err := catserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for catalog to come up
	st.catalog = <-up
	st.catalogAddr = addr
	cl, err := catclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.catalogClient = cl
}

func newCatalogConfig(params *parameters) (*catserver.Config, *net.TCPAddr) {
	startPort := 10200
	storageParams := catstorage.NewDefaultParameters()
	storageParams.Type = bstorage.DataStore
	serverPort, metricsPort := startPort, startPort+1
	config := catserver.NewDefaultConfig().
		WithStorage(storageParams).
		WithGCPProjectID(params.gcpProjectID)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.catalogLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}

func createAndStartKey(params *parameters, st *state) {
	config, addr := newKeyConfig(params)
	up := make(chan *keyserver.Key, 1)

	go func() {
		err := keyserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for key to come up
	st.key = <-up
	st.keyAddr = addr
	cl, err := keyclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.keyClient = cl
}

func newKeyConfig(params *parameters) (*keyserver.Config, *net.TCPAddr) {
	startPort := 10300
	storageParams := keystorage.NewDefaultParameters()
	storageParams.Type = bstorage.DataStore
	serverPort, metricsPort := startPort, startPort+1
	config := keyserver.NewDefaultConfig().
		WithStorage(storageParams).
		WithGCPProjectID(params.gcpProjectID)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.keyLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}

func createAndStartDirectory(params *parameters, st *state) {
	config, addr := newDirectoryConfigs(params, st)
	up := make(chan *dirserver.Directory, 1)

	go func() {
		err := dirserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for server to come up
	st.directory = <-up
	st.directoryAddr = addr
	cl, err := dirclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.directoryClient = cl
}

func newDirectoryConfigs(params *parameters, st *state) (*dirserver.Config, *net.TCPAddr) {
	startPort := 10400
	storageParams := storage.NewDefaultParameters()
	storageParams.Type = bstorage.Postgres

	serverPort, metricsPort := startPort, startPort+1
	config := dirserver.NewDefaultConfig().
		WithStorage(storageParams).
		WithDBUrl(st.dbURL)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.directoryLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}

func createAndStartUser(params *parameters, st *state) {
	config, addr := newUserConfigs(params, st)
	up := make(chan *userserver.User, 1)

	go func() {
		err := userserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for server to come up
	st.user = <-up
	st.userAddr = addr
	cl, err := userclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.userClient = cl
}

func newUserConfigs(params *parameters, _ *state) (*userserver.Config, *net.TCPAddr) {
	startPort := 10500
	storageParams := userstorage.NewDefaultParameters()
	storageParams.Type = bstorage.DataStore

	serverPort, metricsPort := startPort, startPort+1
	config := userserver.NewDefaultConfig().
		WithStorage(storageParams).
		WithGCPProjectID(params.gcpProjectID)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.userLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}
