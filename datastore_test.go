package datastore_test

import (
	"testing"
	"time"

	"github.com/regrowag/ses/pkg/datastore"
)

// Each of these platform services should be represented in the
// platform docker-compose file so that the tests can be run.
const (
	mongoDSN1    = "mongodb://localhost:27018"
	mongoDSN2    = "mongodb://localhost:27019"
	mysqlDSN1    = "root:pass@tcp(localhost:3307)/mysql"
	mysqlDSN2    = "root:pass@tcp(localhost:3308)/mysql"
	postgresDSN1 = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
	postgresDSN2 = "postgres://postgres:postgres@localhost:5434/postgres?sslmode=disable"
	redisDSN     = "redis://:@localhost:6380/0"

	// for all, where relevant
	timeoutSecs = 10
)

func TestPlatform(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping datastore integration tests")
	}
	t.Run("tests", func(t *testing.T) {
		t.Run("testAddMongoConnection", testAddMongoConnection)
		t.Run("testAddMySQLConnection", testAddMySQLConnection)
		t.Run("testAddPostgresConnection", testAddPostgresConnection)
		t.Run("testAddRedisConnection", testAddRedisConnection)
	})
}

func testAddMongoConnection(t *testing.T) {
	cases := []struct {
		dsn string
		key string
		db  string
	}{
		{mongoDSN1, "mongo-1", "db-1"}, // first conn to first server
		{mongoDSN1, "mongo-2", "db-2"}, // second conn to first server
		{mongoDSN2, "mongo-3", "db-3"}, // first conn to second server
		{mongoDSN2, "mongo-4", "db-4"}, // second conn to second server
	}
	conns := datastore.NewWithRetries(5, 1*time.Second)
	for _, c := range cases {
		if err := conns.AddMongoConnection(c.key, c.dsn, c.db); err != nil {
			t.Fatalf(".AddMongoConnection(%s, %s, %s) err = %s", c.key, c.dsn, c.db, err)
		}
	}
	// Check number of connections
	want := 4
	got := len(conns.Mongo)
	if got != want {
		t.Errorf("Number of connections = %d, want %d", got, want)
	}
}

func testAddMySQLConnection(t *testing.T) {
	cases := []struct {
		dsn string
		key string
	}{
		{mysqlDSN1, "mysql-1"}, // first conn to first server
		{mysqlDSN1, "mysql-2"}, // second conn to first server
		{mysqlDSN2, "mysql-3"}, // first conn to second server
		{mysqlDSN2, "mysql-4"}, // second conn to second server
	}

	conns := datastore.NewWithRetries(5, 1*time.Second)
	for _, c := range cases {
		if err := conns.AddMySQLConnection(c.key, c.dsn); err != nil {
			t.Fatalf(".AddMySQLConnection(%s, %s) err = %s", c.key, c.dsn, err)
		}
	}

	// Check number of connections
	want := 4
	got := len(conns.MySQL)
	if got != want {
		t.Errorf("Number of connections = %d, want %d", got, want)
	}
}

func testAddPostgresConnection(t *testing.T) {
	cases := []struct {
		dsn string
		key string
	}{
		{postgresDSN1, "postgres-1"}, // first conn to first server
		{postgresDSN1, "postgres-2"}, // second conn to first server
		{postgresDSN2, "postgres-3"}, // first conn to second server
		{postgresDSN2, "postgres-4"}, // second conn to second server
	}

	conns := datastore.NewWithRetries(5, 1*time.Second)
	for _, c := range cases {
		if err := conns.AddPostgresConnection(c.key, c.dsn); err != nil {
			t.Fatalf(".AddPostgresConnection(%s, %s) err = %s", c.key, c.dsn, err)
		}
	}

	// Check number of connections
	want := 4
	got := len(conns.Postgres)
	if got != want {
		t.Errorf("Number of connections = %d, want %d", got, want)
	}
}

func testAddRedisConnection(t *testing.T) {
	cases := []struct {
		dsn string
		key string
	}{
		{redisDSN, "redis-1"},
	}
	conns := datastore.NewWithRetries(5, 1*time.Second)
	for _, c := range cases {
		if err := conns.AddRedisConnection(c.key, c.dsn, timeoutSecs); err != nil {
			t.Fatalf(".AddRedisConnection(%s, %s) err = %s", c.key, c.dsn, err)
		}
	}

	// Check number of connections
	want := 1
	got := len(conns.Redis) // first conn to first server
	if got != want {
		t.Errorf("Number of connections = %d, want %d", got, want)
	}
}
