package apitest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestDriver struct{}

func (d *TestDriver) Open(name string) (driver.Conn, error) { return &TestConn{}, nil }

type TestDriverWithConnector struct {
	TestDriver
}

func (d *TestDriverWithConnector) OpenConnector(name string) (driver.Connector, error) {
	return &TestConnector{}, nil
}

type TestConnector struct{}

func (c *TestConnector) Connect(context.Context) (driver.Conn, error) { return &TestConn{}, nil }
func (c *TestConnector) Driver() driver.Driver                        { return &TestDriverWithConnector{} }

type TestConn struct{}

func (c *TestConn) Prepare(query string) (driver.Stmt, error) { return &TestStmt{}, nil }
func (c *TestConn) Close() error                              { return nil }
func (c *TestConn) Begin() (driver.Tx, error)                 { return &TestTx{}, nil }

type TestConnPrepareCtx struct {
	*TestConn
}

func (c *TestConnPrepareCtx) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &TestStmt{}, nil
}

type TestTx struct{}

func (t *TestTx) Commit() error   { return nil }
func (t *TestTx) Rollback() error { return nil }

type TestStmt struct{}

func (s *TestStmt) Close() error                                    { return nil }
func (s *TestStmt) NumInput() int                                   { return 0 }
func (s *TestStmt) Exec(args []driver.Value) (driver.Result, error) { return &TestResult{}, nil }
func (s *TestStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return &TestResult{}, nil
}
func (s *TestStmt) Query(args []driver.Value) (driver.Rows, error)  { return &TestRows{}, nil }
func (s *TestStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return &TestRows{}, nil
}

type TestRows struct{}

func (r *TestRows) Columns() []string              { return nil }
func (r *TestRows) Close() error                   { return nil }
func (r *TestRows) Next(dest []driver.Value) error { return io.EOF }

type TestResult struct{}

func (r *TestResult) LastInsertId() (int64, error) { return 0, nil }
func (r *TestResult) RowsAffected() (int64, error) { return 0, nil }

func Test_Query_RecordsMessageRequestResponse(t *testing.T) {
	recorder := NewTestRecorder()
	recordingTestDriver := &RecordingDriver{
		sourceName: "source",
		Driver:     &TestDriver{},
		recorder:   recorder,
	}
	sql.Register("withRecorder", recordingTestDriver)

	db, err := sql.Open("withRecorder", "dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name, age FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, recorder.Events, 2)
}

func Test_QueryContext_RecordsMessageRequestResponse(t *testing.T) {
	recorder := NewTestRecorder()
	recordingTestDriver := &RecordingDriver{
		sourceName: "source",
		Driver:     &TestDriver{},
		recorder:   recorder,
	}
	sql.Register("withRecorder", recordingTestDriver)

	db, err := sql.Open("withRecorder", "dsn")
	assert.NoError(t, err)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT id, name, age FROM users WHERE id = 1")
	assert.NoError(t, err)
	defer rows.Close()
	for rows.Next() {}
	assert.NoError(t, rows.Err())

	assert.Len(t, recorder.Events, 2)
}

func Test_Exec_RecordsMessageRequestResponse(t *testing.T) {
	recorder := NewTestRecorder()
	recordingTestDriver := &RecordingDriver{
		sourceName: "source",
		Driver:     &TestDriver{},
		recorder:   recorder,
	}
	sql.Register("withRecorder", recordingTestDriver)

	db, err := sql.Open("withRecorder", "dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DELETE FROM users WHERE id = 1")

	assert.Len(t, recorder.Events, 2)
}

func Test_ExecContext_RecordsMessageRequestResponse(t *testing.T) {
	recorder := NewTestRecorder()
	recordingTestDriver := &RecordingDriver{
		sourceName: "source",
		Driver:     &TestDriver{},
		recorder:   recorder,
	}
	sql.Register("withRecorder", recordingTestDriver)

	db, err := sql.Open("withRecorder", "dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.ExecContext(context.Background(),"DELETE FROM users WHERE id = 1")

	assert.Len(t, recorder.Events, 2)
}

func Test_Prepare_RecordsMessageRequestResponse(t *testing.T) {
	recorder := NewTestRecorder()
	recordingTestDriver := &RecordingDriver{
		sourceName: "source",
		Driver:     &TestDriver{},
		recorder:   recorder,
	}
	sql.Register("withRecorder", recordingTestDriver)

	db, err := sql.Open("withRecorder", "dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare("INSERT INTO users(name, age) VALUES(?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	stmt.Exec("Peter", "24")

	assert.Len(t, recorder.Events, 2)
}