package ws

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/libsql/libsql-client-go/libsql"
)

func Test_QueryExec(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Fatal("id should be 1")
		}
		if name != "hello world" {
			t.Fatal("name should be hello world")
		}
	}

	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_Transactions(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Fatal("id should be 1")
		}
		if name != "hello world" {
			t.Fatal("name should be hello world")
		}
	}

	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_PreparedStatements(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := db.PrepareContext(ctx, "INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Fatal("id should be 1")
		}
		if name != "hello world" {
			t.Fatal("name should be hello world")
		}
	}

	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_PreparedStatementsWithTransactions(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Fatal("id should be 1")
		}
		if name != "hello world" {
			t.Fatal("name should be hello world")
		}
	}

	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_TransactionsRollback(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("count should be 0")
	}

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_PreparedStatementsWithTransactionsRollback(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("count should be 0")
	}

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_CancelContext(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err == nil {
		t.Fatal("should have failed")
	}
	db.Close()
}

func Test_CancelTransactionWithContext(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO test (name) VALUES (?)", "hello world")
	if err != nil {
		t.Fatal(err)
	}
	// let's cancel the context before the commit
	cancel()
	err = tx.Commit()
	if err == nil {
		t.Fatal("should have failed")
	}
	// rolling back the transaction should not result in any error
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = db.ExecContext(context.Background(), "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}

func Test_DataTypes(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	var (
		text        string
		nullText    sql.NullString
		integer     sql.NullInt64
		nullInteger sql.NullInt64
		boolean     bool
		float8      float64
		bytea       []byte
	)
	err = db.QueryRowContext(ctx, "SELECT 'foobar' as text, NULL as text,  NULL as integer, 42 as integer, 1 as boolean, X'000102' as bytea, 3.14 as float8;").Scan(&text, &nullText, &nullInteger, &integer, &boolean, &bytea, &float8)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case text != "foobar":
		t.Fatal("value mismatch")
	}
}

// TODO: This doesn't pass yet
func Test_TimeStamp(t *testing.T) {
	dbURL := os.Getenv("LIBSQL_TEST_DB_URL")
	db, err := sql.Open("libsql", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var timeStamp, timeNow time.Time
	err = db.QueryRowContext(ctx, "SELECT datetime('2023-01-01 01:02:03.040', '-7 hours') as timeStamp, datetime('now') as timenow;").Scan(&timeStamp, &timeNow)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, created_at DATETIME)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO test (created_at) VALUES (?)", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var id int
	var createdAt time.Time
	err = db.QueryRowContext(ctx, "SELECT id, created_at FROM test").Scan(&id, &createdAt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, "DROP TABLE test")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
}
