// Package datastore provides a structure within which to store connections to multiple data sources and services.
package datastore

import (
	"fmt"
	"time"

	"github.com/regrowag/ses/pkg/retry"
)

const (
	defaultConnectRetries     = 5
	defaultConnectWaitSeconds = 2
)

// Connections represents connections to various platform services, most likely databases.
type Connections struct {
	Mongo    map[string]*MongoConnection
	MySQL    map[string]*MySQLConnection
	Postgres map[string]*PostgresConnection
	Redis    map[string]*RedisConnection
	Retry    *retry.Attempts
}

// New returns a pointer to a Connections value no connections attached.
func New() *Connections {
	return &Connections{
		Mongo:    make(map[string]*MongoConnection),
		MySQL:    make(map[string]*MySQLConnection),
		Postgres: make(map[string]*PostgresConnection),
		Redis:    make(map[string]*RedisConnection),
	}
}

// NewWithRetries returns a pointer to a Connections value with the specified number of retries and wait time.
func NewWithRetries(retries uint, wait time.Duration) *Connections {
	return &Connections{
		Mongo:    make(map[string]*MongoConnection),
		MySQL:    make(map[string]*MySQLConnection),
		Postgres: make(map[string]*PostgresConnection),
		Redis:    make(map[string]*RedisConnection),
		Retry:    retry.CountWithWait(retries, wait),
	}
}

// ensureRetries ensures that the Retry field is not nil.
func (mc *Connections) ensureRetries() {
	if mc.Retry == nil {
		mc.Retry = retry.CountWithWait(defaultConnectRetries, defaultConnectWaitSeconds*time.Second)
	}
}

// AddMongoConnection adds a connection to a mongo database identified by the specified key.
func (mc *Connections) AddMongoConnection(key, dsn, db string) error {
	mc.ensureRetries()
	var err error
	var c *MongoConnection
	for mc.Retry.Next() {
		c, err = connectMongo(dsn, db)
		if err == nil {
			mc.Mongo[key] = c
			break
		}
	}
	return err
}

// MongoConnByKey returns the mongodb connection value at the specified key.
func (mc *Connections) MongoConnByKey(key string) (*MongoConnection, error) {
	c, ok := mc.Mongo[key]
	if !ok {
		return nil, fmt.Errorf("no mongodb connection with key = %s", key)
	}
	return c, nil
}

// OnlyMongoConnection is a convenience function that returns the Mongo connection if there is only one.
func (mc *Connections) OnlyMongoConnection() (*MongoConnection, error) {
	num := len(mc.Mongo)
	if num != 1 {
		return nil, fmt.Errorf("cannot return unique mongo connection as %d exist", num)
	}
	var key string
	for k := range mc.Mongo { // there's only one
		key = k
	}
	return mc.Mongo[key], nil
}

// AddMySQLConnection adds a connection to a mysql database identified by the specified key.
func (mc *Connections) AddMySQLConnection(key, dsn string) error {
	mc.ensureRetries()
	var err error
	var c *MySQLConnection
	for mc.Retry.Next() {
		c, err = connectMySQL(dsn)
		if err == nil {
			mc.MySQL[key] = c
			break
		}
	}
	return err
}

// MySQLConnByKey returns the MySQL connection value at the specified key.
func (mc *Connections) MySQLConnByKey(key string) (*MySQLConnection, error) {
	c, ok := mc.MySQL[key]
	if !ok {
		return nil, fmt.Errorf("no MySQL db connection with key = %s", key)
	}
	return c, nil
}

// OnlyMySQLConnection is a convenience function that returns the MySQL connection if there is only one.
func (mc *Connections) OnlyMySQLConnection() (*MySQLConnection, error) {
	num := len(mc.MySQL)
	if num != 1 {
		return nil, fmt.Errorf("cannot return unique MySQL connection as %d exist", num)
	}
	var key string
	for k := range mc.MySQL {
		key = k
	}
	return mc.MySQL[key], nil
}

// AddPostgresConnection adds a connection to a potsgres database identified by the specified key.
func (mc *Connections) AddPostgresConnection(key, dsn string) error {
	mc.ensureRetries()
	var err error
	var c *PostgresConnection
	for mc.Retry.Next() {
		c, err = connectPostgres(dsn)
		if err == nil {
			mc.Postgres[key] = c
			break
		}
	}
	return err
}

// PostgresConnByKey returns the postgres connection value at the specified key.
func (mc *Connections) PostgresConnByKey(key string) (*PostgresConnection, error) {
	c, ok := mc.Postgres[key]
	if !ok {
		return nil, fmt.Errorf("no postgresdb connection with key = %s", key)
	}
	return c, nil
}

// OnlyPostgresConnection is a convenience function that returns the Postgres connection if there is only one.
func (mc *Connections) OnlyPostgresConnection() (*PostgresConnection, error) {
	num := len(mc.Postgres)
	if num != 1 {
		return nil, fmt.Errorf("cannot return unique postgres connection as %d exist", num)
	}
	var key string
	for k := range mc.Postgres {
		key = k
	}
	return mc.Postgres[key], nil
}

// AddRedisConnection adds a connection to a redis database identified by the specified key.
func (mc *Connections) AddRedisConnection(key, dsn string, timeoutSeconds int) error {
	mc.ensureRetries()
	var err error
	var c *RedisConnection
	for mc.Retry.Next() {
		c, err = connectRedis(dsn, timeoutSeconds)
		if err == nil {
			mc.Redis[key] = c
			break
		}
	}
	return err
}

// RedisConnByKey returns the Redis connection value at the specified key.
func (mc *Connections) RedisConnByKey(key string) (*RedisConnection, error) {
	c, ok := mc.Redis[key]
	if !ok {
		return nil, fmt.Errorf("no Redisdb connection with key = %s", key)
	}
	return c, nil
}

// OnlyRedisConnection is a convenience function that returns the Redis connection if there is only one.
func (mc *Connections) OnlyRedisConnection() (*RedisConnection, error) {
	num := len(mc.Redis)
	if num != 1 {
		return nil, fmt.Errorf("cannot return unique Redis connection as %d exist", num)
	}
	var key string
	for k := range mc.Redis {
		key = k
	}
	return mc.Redis[key], nil
}
