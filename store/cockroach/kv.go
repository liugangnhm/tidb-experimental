package cockroach

import (
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	cockroach "github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

const (
	// TiDB start key prefix
	tidbStartKeyPrefix = "tidb"
	// TiDB end key prefix
	tidbEndKeyPrefix = "tidc"
	// fix length conn pool
	cockroachConnPollSize = 10
	// CockroachScheme cockroach scheme
	CockroachScheme = "cockroach"
)

var (
	_ kv.Storage = (*cockroachStore)(nil)
)

var (
	// ErrInvalidDSN is returned when store dsn is invalid.
	ErrInvalidDSN = errors.New("invalid dsn")
	// ErrInvalidKey is returned when key is invalid.
	ErrInvalidKey = errors.New("invalid key")
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*cockroachStore
}

var mc storeCache

func init() {
	mc.cache = make(map[string]*cockroachStore)
	rand.Seed(time.Now().UnixNano())
}

type cockroachClient struct {
	db      *cockroach.DB
	stopper *stop.Stopper
}

type cockroachStore struct {
	mu     sync.Mutex
	uuid   string
	sender string
	user   string
	host   string
	port   int
	params string
	conns  []cockroachClient
}

func (s *cockroachStore) getCockroachClient() cockroachClient {
	// return cockroach connection randomly
	return s.conns[rand.Intn(cockroachConnPollSize)]
}

func (s *cockroachStore) Begin() (kv.Transaction, error) {
	cockroachCli := s.getCockroachClient()
	t := cockroach.NewTxn(*cockroachCli.db)
	return newCockroachTxn(t), nil
}

func (s *cockroachStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	cockroachCli := s.getCockroachClient()
	t := cockroach.NewTxn(*cockroachCli.db)
	return newCockroachSnapshot(t), nil
}

func (s *cockroachStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.uuid)
	for _, conn := range s.conns {
		conn.stopper.Stop()
		conn.stopper = nil
	}
	//return last error
	return nil
}

func (s *cockroachStore) UUID() string {
	return s.uuid
}

func (s *cockroachStore) CurrentVersion() (kv.Version, error) {
	// TODO: CockroachDB has no interface to get version information.
	return kv.Version{Ver: 1}, nil
}

// Driver implements engine Driver.
type Driver struct {
}

// Open connects to a running cockroach cluster with given path.
// The format of path shoud be '[<engine>:]//[<user>@]<host>:<port>[?certs=<dirs>,priority=<val>]'.
// The URL scheme (<engine>) specifies which transport to use for talking to the cockroach cluster.
//
// If not specified, the <user> field defaults to "root".
//
// The certs parameter can be used to override the default directory to use for client certicates.
//
// The priority parameter can be override the default priority for operations.
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	sender, user, host, port, params, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	uuid := fmt.Sprintf("cockroachdb-%v-%v-%v-%v", sender, user, host, port)
	if store, ok := mc.cache[uuid]; ok {
		if host != store.host && port != store.port {
			err = errors.Errorf("cockroachdb: store(%s) is opened with a different host:port, old: %v:%v, new: %v:%v",
				uuid, store.host, store.port, host, port)
			return nil, errors.Trace(err)
		}
		return store, nil
	}

	var addr string
	if params == "" {
		addr = fmt.Sprintf("%s://%s@%s:%d", sender, user, host, port)
	} else {
		addr = fmt.Sprintf("%s://%s@%s:%d?%s", sender, user, host, port, params)
	}
	log.Debugf("Open cockroach cluster with addr %s", addr)

	// create buffered CockroachDB connections, cockroach.DB is goroutine-safe, so
	// it's OK to redistribute to transaction.
	conns := make([]cockroachClient, 0, cockroachConnPollSize)
	for i := 0; i < cockroachConnPollSize; i++ {
		var c cockroachClient
		c.stopper = stop.NewStopper()
		c.db, err = cockroach.Open(c.stopper, addr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		conns = append(conns, c)
	}

	s := &cockroachStore{
		uuid:   uuid,
		sender: sender,
		user:   user,
		host:   host,
		port:   port,
		params: params,
		conns:  conns,
	}
	mc.cache[uuid] = s
	return s, nil
}

func parsePath(path string) (sender, user, host string, port int, params string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", "", "", 0, "", errors.Trace(err)
	}

	if strings.ToLower(u.Scheme) != CockroachScheme {
		log.Debugf("error scheme %s", u.Scheme)
		err = errors.Trace(ErrInvalidDSN)
		return
	}
	log.Debugf("[kv] url:%#v", u)
	sender = "rpcs"
	if u.User == nil {
		user = "node"
	} else {
		user = u.User.Username()
	}
	hostStrs := strings.Split(u.Host, ":")
	if len(hostStrs) != 2 {
		log.Debugf("error host %s %d", hostStrs, len(hostStrs))
		err = errors.Trace(ErrInvalidDSN)
		return
	}
	port, err = strconv.Atoi(hostStrs[1])
	if err != nil || port <= 0 || port >= 65536 {
		log.Debugf("error port %d", port)
		err = errors.Trace(ErrInvalidDSN)
		return
	}
	host = hostStrs[0]
	params = u.RawQuery
	err = nil
	return
}
