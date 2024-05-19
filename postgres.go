package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type PostgresConnection struct {
	DSN string
	*sql.DB
}

// newPostgresConnection returns a pointer to a PostgresConnection with an established
// db session, or an error.
func newPostgresConnection(dsn string) (*PostgresConnection, error) {
	conn := PostgresConnection{
		DSN: dsn,
		DB:  nil,
	}
	db, err := conn.open()
	if err != nil {
		return nil, fmt.Errorf("could not open postgres db connection, err = %w", err)
	}
	conn.DB = db
	if err := conn.Check(); err != nil {
		return nil, fmt.Errorf("postgres db connection check failed, err = %w", err)
	}
	return &conn, nil
}

// connectPostgres returns a postgres connection or an error.
func connectPostgres(dsn string) (*PostgresConnection, error) {
	return newPostgresConnection(dsn)
}

// open returns a connection to the database or an error.
func (c *PostgresConnection) open() (*sql.DB, error) {
	return sql.Open("postgres", c.DSN)
}

// Check verifies the connection to the database and returns an error if there's a problem.
// Can pass in one or more statements to check the db in whatever way suits.
func (c *PostgresConnection) Check(stmts ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if len(stmts) == 0 {
		_, err := c.DB.ExecContext(ctx, `SELECT true`)
		return err
	}
	for _, stmt := range stmts {
		log.Println("checking postgres with stmt:", stmt)
		_, err := c.DB.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed postgres check on stmt %s: %w", stmt, err)
		}
	}
	return nil
}

// connectRedis returns a redis connection or an error.
func connectRedis(dsn string, timeoutSeconds int) (*RedisConnection, error) {
	return newRedisConnection(dsn, timeoutSeconds)
}

// // open returns a connection to the database or an error.
// func (c *postgresConnection) open() (*sql.DB, error) {
//	return sql.Open("postgres", c.DSN)
// }
//
// // Check verifies the connection to the database and returns an error if there's a problem.
// // Note: This is better than ping because it forces a round trip to the database.
// func (c *postgresConnection) Check() error {
//	var tmp bool
//	return c.DB.QueryRow(`SELECT true`).Scan(&tmp)
// }
