package db

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	toml "github.com/pelletier/go-toml"
)

// DB class
type DB struct {
	connection *sql.DB
}

var sessions = map[string]*DB{}

var cfg *toml.TomlTree

var configPath string

// ConfigPath custom setting configuration path
func ConfigPath(path string) {
	if path != "" {
		configPath = path
	} else {
		execDirAbsPath, _ := os.Getwd()
		configPath = execDirAbsPath + "/config"
	}
}

// getConfigPath return configPath
func getConfigPath() string {
	if configPath == "" {
		ConfigPath("")
	}
	return configPath
}

func loadConfig() *toml.TomlTree {
	if cfg != nil {
		return cfg
	}
	cfg, err := toml.LoadFile(getConfigPath() + "/database.toml")
	if err != nil {
		// database.toml error
		panic(err.Error())
	}
	return cfg
}

// Prepare sql.Db Prepare
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.connection.Prepare(query)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.connection.Exec(query, args)
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.connection.Query(query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.connection.QueryRow(query, args...)
}

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.connection.Begin()
}

// Select for query
func (db *DB) Select(query string, args ...interface{}) ([]map[string]string, error) {
	rows, err := db.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var data []map[string]string
	var row map[string]string

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row = make(map[string]string)
		for i, v := range values {
			if v == nil {
				row[columns[i]] = ""
			} else {
				row[columns[i]] = string(v)
			}

		}

		data = append(data, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return data, nil
}

// Session a mysql client session by datasoucre name
func Session(DSN string) *DB {
	if session, ok := sessions[DSN]; ok {
		return session
	}

	conf := loadConfig()
	dsn := conf.Get("database." + DSN + ".dsn").(string)
	fmt.Println("dsn is :", DSN)
	if dsn == "" {
		panic("datasource name(" + DSN + ") is no set")
	}
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("ok")

	sessions[DSN] = &DB{
		connection: conn,
	}

	return sessions[DSN]
}

// Close close all sessions
func Close() {
	fmt.Println("close db ")

	for DSN, session := range sessions {
		fmt.Println(DSN)
		session.connection.Close()
		delete(sessions, DSN)
	}
}
