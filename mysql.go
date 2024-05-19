package datastore

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLConnection struct {
	DSN string
	*sql.DB
}

// newMySQLConnection returns a pointer to a MySQLConnection with an established db session, or an error.
func newMySQLConnection(dsn string) (*MySQLConnection, error) {
	conn := MySQLConnection{
		DSN: dsn,
		DB:  nil,
	}
	db, err := conn.open()
	if err != nil {
		return nil, fmt.Errorf("could not open mysql db connection, err = %w", err)
	}
	conn.DB = db
	if err := conn.Check(); err != nil {
		return nil, fmt.Errorf("mysql db connection check failed, err = %w", err)
	}
	return &conn, nil
}

// connectMySQL returns a mysql connection or an error.
func connectMySQL(dsn string) (*MySQLConnection, error) {
	return newMySQLConnection(dsn)
}

// open returns a connection to the database or an error.
func (c *MySQLConnection) open() (*sql.DB, error) {
	return sql.Open("mysql", c.DSN)
}

// Check verifies the connection to the database and returns an error if there's a problem.
// Note: This is better than ping because it forces a round trip to the database.
func (c *MySQLConnection) Check() error {
	var tmp bool
	return c.DB.QueryRow(`SELECT true`).Scan(&tmp)
}
