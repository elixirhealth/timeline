// +build acceptance

package acceptance

import (
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	"github.com/elxirhealth/catalog/pkg/catalogapi"
	catclient "github.com/elxirhealth/catalog/pkg/client"
	catserver "github.com/elxirhealth/catalog/pkg/server"
	catstorage "github.com/elxirhealth/catalog/pkg/server/storage"
	"github.com/elxirhealth/courier/pkg/cache"
	cclient "github.com/elxirhealth/courier/pkg/client"
	"github.com/elxirhealth/courier/pkg/courierapi"
	cserver "github.com/elxirhealth/courier/pkg/server"
	dirclient "github.com/elxirhealth/directory/pkg/client"
	dirapi "github.com/elxirhealth/directory/pkg/directoryapi"
	dirserver "github.com/elxirhealth/directory/pkg/server"
	"github.com/elxirhealth/directory/pkg/server/storage"
	"github.com/elxirhealth/directory/pkg/server/storage/postgres/migrations"
	keyclient "github.com/elxirhealth/key/pkg/client"
	"github.com/elxirhealth/key/pkg/keyapi"
	keystorage "github.com/elxirhealth/key/pkg/server/storage"
	bstorage "github.com/elxirhealth/service-base/pkg/server/storage"
	api "github.com/elxirhealth/timeline/pkg/timelineapi"
	userclient "github.com/elxirhealth/user/pkg/client"
	userserver "github.com/elxirhealth/user/pkg/server"
	userstorage "github.com/elxirhealth/user/pkg/server/storage"
	"github.com/elxirhealth/user/pkg/userapi"
	"github.com/mattes/migrate/source/go-bindata"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"

	keyserver "github.com/elxirhealth/key/pkg/server"
)

const (
	datastoreEmulatorHostEnv = "DATASTORE_EMULATOR_HOST"
)

type parameters struct {
	nTimelines int

	rqTimeout     time.Duration
	gcpProjectID  string
	datastoreAddr string

	timelineLogLevel zapcore.Level

	catalogLogLevel   zapcore.Level
	courierLogLevel   zapcore.Level
	directoryLogLevel zapcore.Level
	keyLogLevel       zapcore.Level
	userLogLevel      zapcore.Level
}

type state struct {
	courier   *cserver.Courier
	catalog   *catserver.Catalog
	directory *dirserver.Directory
	key       *keyserver.Key
	user      *userserver.User

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

	rng               *rand.Rand
	dataDir           string
	events            map[string][]*api.Event
	dbURL             string
	datastoreEmulator *os.Process
	tearDownPostgres  func() error
}

func TestAcceptance(t *testing.T) {
	params := &parameters{
		nTimelines:        3,
		rqTimeout:         1 * time.Second,
		datastoreAddr:     "localhost:2001",
		timelineLogLevel:  zapcore.InfoLevel,
		catalogLogLevel:   zapcore.InfoLevel,
		courierLogLevel:   zapcore.InfoLevel,
		directoryLogLevel: zapcore.InfoLevel,
		keyLogLevel:       zapcore.InfoLevel,
		userLogLevel:      zapcore.InfoLevel,
	}
	st := setUp(params)

	// create users in user service
	// create entities for users in directory service
	// create docs in courier
	// create publications for entities catalog

	// get timelines for different users

	tearDown(t, st)
}

func createEvents(t *testing.T, params *parameters, st *state) {

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

	return nil
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
	log.Printf("datastore PID: %d\n", st.datastoreEmulator.Pid)
	os.Setenv(datastoreEmulatorHostEnv, params.datastoreAddr)
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
		WithGCPProjectID(params.gcpProjectID).
		WithNLibriPutters(0)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(serverPort + 1)).
		WithLogLevel(params.courierLogLevel)
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
	directory := <-up
	st.directory = directory
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
	user := <-up
	st.user = user
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
