package cockroach

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	roachserver "github.com/cockroachdb/cockroach/server"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

func init() {
	security.SetReadFileFn(securitytest.Asset)
}

func setup(c *C) kv.Storage {
	var (
		d   Driver
		err error
	)

	s := &roachserver.TestServer{}
	ctx := roachserver.NewTestContext()
	ctx.MaxOffset = 0
	s.Ctx = ctx
	err = s.Start()
	c.Assert(err, IsNil, Commentf("Could not start server: %v", err))

	path := fmt.Sprintf("cockroach://%s@%s?certs=%s",
		security.NodeUser,
		s.ServingAddr(),
		security.EmbeddedCertsDir)
	store, err := d.Open(path)
	c.Assert(err, IsNil, Commentf("dsn=%v", path))
	return store
}

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testcockroachSuite{})

type testcockroachSuite struct {
}

func (t *testcockroachSuite) TestParsePath(c *C) {
	tbl := []struct {
		dsn    string
		ok     bool
		sender string
		user   string
		host   string
		port   int
		params string
	}{
		{"cockroach://localhost:30000", true, "rpcs", "node", "localhost", 30000, ""},
		{"cockroach://nieyy@code.huawei.com:12345", true, "rpcs", "nieyy", "code.huawei.com", 12345, ""},
		{"cockroach://127.0.0.1:5432?certs=/tmp/test_cert", true, "rpcs", "node", "127.0.0.1", 5432, "certs=/tmp/test_cert"},
		{"cockroach://test@127.0.0.1:5432?certs=/tmp/test_cert,priority=10", true, "rpcs", "test", "127.0.0.1", 5432, "certs=/tmp/test_cert,priority=10"},
		{"hbase://zk/tbl", false, "", "", "", 0, ""},
		{"cockroach://localhost:0", false, "", "", "", 0, ""},
		{"cockroach://localhost:65536", false, "", "", "", 0, ""},
		{"cockroachs://localhost:5432", false, "", "", "", 0, ""},
		{"cockroach://localhost", false, "", "", "", 0, ""},
	}

	for _, t := range tbl {
		sender, user, host, port, params, err := parsePath(t.dsn)
		if t.ok {
			c.Assert(err, IsNil, Commentf("dsn=%v", t.dsn))
			c.Assert(sender, Equals, t.sender, Commentf("dsn=%v", t.dsn))
			c.Assert(user, Equals, t.user, Commentf("dsn=%v", t.dsn))
			c.Assert(host, Equals, t.host, Commentf("dsn=%v", t.dsn))
			c.Assert(port, Equals, t.port, Commentf("dsn=%v", t.dsn))
			c.Assert(params, Equals, t.params, Commentf("dsn=%v", t.dsn))
		} else {
			c.Assert(err, NotNil, Commentf("dsn=%v", t.dsn))
		}
	}
}

func (t *testcockroachSuite) TestDriverOpen(c *C) {
	store := setup(c)
	defer store.Close()
}

func bytes(str string) []byte {
	return []byte(str)
}

func (t *testcockroachSuite) TestGetSet(c *C) {
	store := setup(c)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))

	err = txn.Set([]byte("a"), []byte("1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("2"))
	c.Assert(err, IsNil)
	err = txn.Delete([]byte("c"))
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))

	v, err := txn.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("1"))

	v, err = txn.Get([]byte("c"))
	// Key not exitst error
	c.Assert(err, NotNil)
	c.Assert(v, IsNil)

	err = txn.Set([]byte("a"), []byte("2"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))

	v, err = txn.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("2"))

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (t *testcockroachSuite) TestSeek(c *C) {
	store := setup(c)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	err = txn.Set(bytes("a"), bytes("1"))
	c.Assert(err, IsNil)
	err = txn.Set(bytes("b"), bytes("2"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	iter, err := txn.Seek(nil)
	c.Assert(err, IsNil)
	c.Assert(string(iter.Key()), Equals, "a")
	c.Assert(iter.Value(), BytesEquals, bytes("1"))
	iter.Close()

	iter, err = txn.Seek(bytes("a"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(string(iter.Key()), Equals, "a")
	c.Assert(iter.Value(), BytesEquals, bytes("1"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(string(iter.Key()), Equals, "b")
	c.Assert(iter.Value(), BytesEquals, bytes("2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()

	iter, err = txn.Seek(bytes("b"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(string(iter.Key()), Equals, "b")
	c.Assert(iter.Value(), BytesEquals, bytes("2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()

	iter, err = txn.Seek(bytes("a1"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(string(iter.Key()), Equals, "b")
	c.Assert(iter.Value(), BytesEquals, bytes("2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()

	iter, err = txn.Seek(bytes("c1"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	err = txn.Delete(bytes("a"))
	c.Assert(err, IsNil)
	err = txn.Delete(bytes("b"))
	c.Assert(err, IsNil)
	iter.Close()
	iter, err = txn.Seek(bytes("a"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	iter, err = txn.Seek(bytes("b"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (t *testcockroachSuite) TestLargeSeek(c *C) {
	store := setup(c)
	defer store.Close()
	txn, err := store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	m := make(map[string]string)
	for i := 0; i < 1000; i++ {
		err = txn.Set(bytes(strconv.Itoa(i)), bytes(strconv.Itoa(i)))
		c.Assert(err, IsNil)
		m[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	iter, err := txn.Seek(bytes("0"))
	c.Assert(err, IsNil)
	c.Assert(len(m), Equals, 1000)
	for i := 0; i < 1000; i++ {
		c.Assert(iter.Valid(), IsTrue)
		c.Assert(string(iter.Key()), Equals, string(iter.Value()))
		delete(m, string(iter.Key()))
		c.Assert(iter.Next(), IsNil)
	}
	c.Assert(iter.Valid(), IsFalse)
	c.Assert(len(m), Equals, 0)
	iter.Close()
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	for i := 0; i < 1000; i++ {
		m[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	c.Assert(len(m), Equals, 1000)
	iter, err = txn.Seek(bytes("0"))
	c.Assert(err, IsNil)
	for i := 0; i < 1000; i++ {
		c.Assert(iter.Valid(), IsTrue)
		c.Assert(string(iter.Key()), Equals, string(iter.Value()))
		delete(m, string(iter.Key()))
		c.Assert(iter.Next(), IsNil)
	}
	c.Assert(iter.Valid(), IsFalse)
	c.Assert(len(m), Equals, 0)
	iter.Close()
	for i := 0; i < 1000; i++ {
		err = txn.Delete(bytes(strconv.Itoa(i)))
		c.Assert(err, IsNil)
	}
	iter, err = txn.Seek(bytes("0"))
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	iter, err = txn.Seek(bytes("0"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (t *testcockroachSuite) TestRollback(c *C) {
	store := setup(c)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	err = txn.Set(bytes("l"), bytes("1"))
	c.Assert(err, IsNil)
	err = txn.Set(bytes("j"), bytes("2"))
	c.Assert(err, IsNil)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	iter, err := txn.Seek(bytes("l"))
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	iter, err = txn.Seek(bytes("j"))
	c.Assert(iter.Valid(), IsFalse)
	iter.Close()
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (t *testcockroachSuite) TestBatchGet(c *C) {
	store := setup(c)
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	err = txn.Set(bytes("l"), bytes("l"))
	c.Assert(err, IsNil)
	err = txn.Set(bytes("j"), bytes("j"))
	c.Assert(err, IsNil)
	err = txn.Set(bytes("k"), bytes("k"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	var ver kv.Version
	snapshot, err := store.GetSnapshot(ver)
	c.Assert(err, IsNil, Commentf("failed to get snapshot"))
	var keys []kv.Key
	keys = append(keys, kv.Key(bytes("l")), kv.Key(bytes("j")), kv.Key(bytes("k")))
	m, err := snapshot.BatchGet(keys)
	c.Assert(err, IsNil)
	c.Assert(len(m), Equals, 3)
	c.Assert(m["l"], BytesEquals, bytes("l"))
	c.Assert(m["j"], BytesEquals, bytes("j"))
	c.Assert(m["k"], BytesEquals, bytes("k"))

	txn, err = store.Begin()
	c.Assert(err, IsNil, Commentf("failed to initialize transaction"))
	err = txn.Delete(bytes("l"))
	c.Assert(err, IsNil)
	err = txn.Delete(bytes("j"))
	c.Assert(err, IsNil)
	err = txn.Delete(bytes("k"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	snapshot, err = store.GetSnapshot(ver)
	c.Assert(err, IsNil, Commentf("failed to get snapshot"))
	keys = nil
	keys = append(keys, kv.Key(bytes("l")), kv.Key(bytes("j")), kv.Key(bytes("k")))
	m, err = snapshot.BatchGet(keys)
	c.Assert(err, IsNil)
	c.Assert(len(m), Equals, 0)
}
