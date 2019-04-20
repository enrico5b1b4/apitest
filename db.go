package apitest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

// WrapWithRecorder wraps an existing driver with a Recorder
func WrapWithRecorder(driverName string, recorder *Recorder) driver.Driver {
	sqlDriver := sqlDriverNameToDriver(driverName)
	if _, ok := sqlDriver.(driver.DriverContext); ok {
		return &RecordingDriverContext{
			&RecordingDriver{
				sourceName: driverName,
				Driver:     sqlDriver,
				recorder:   recorder,
			},
		}
	}

	return &RecordingDriver{
		sourceName: driverName,
		Driver:     sqlDriver,
		recorder:   recorder,
	}
}

type RecordingDriver struct {
	Driver     driver.Driver
	recorder   *Recorder
	sourceName string
}

func (d *RecordingDriver) Open(name string) (driver.Conn, error) {
	fmt.Println("Driver Debug: Driver Open")
	conn, err := d.Driver.Open(name)
	if err != nil {
		return conn, err
	}

	_, isConnQuery := conn.(driver.Queryer)
	_, isConnQueryCtx := conn.(driver.QueryerContext)
	_, isConnExec := conn.(driver.Execer)
	_, isConnExecCtx := conn.(driver.ExecerContext)
	_, isConnPrepareCtx := conn.(driver.ConnPrepareContext)
	recordingConn := &RecordingConn{Conn: conn, recorder: d.recorder, sourceName: d.sourceName}

	if isConnQueryCtx && isConnExecCtx && isConnPrepareCtx {
		return &RecordingConnWithExecQueryPrepareContext{
			recordingConn,
			&RecordingConnWithPrepareContext{recordingConn},
			&RecordingConnWithExecContext{recordingConn},
			&RecordingConnWithQueryContext{recordingConn},
			&RecordingConnWithBeginTx{recordingConn},
			&RecordingConnWithPing{recordingConn},
		}, nil
	}

	if isConnQuery && isConnExec {
		return &RecordingConnWithExecQuery{
			recordingConn,
			&RecordingConnWithExec{recordingConn},
			&RecordingConnWithQuery{recordingConn},
		}, nil
	}

	return recordingConn, nil
}

type RecordingDriverContext struct {
	*RecordingDriver
}

func (d *RecordingDriverContext) OpenConnector(name string) (driver.Connector, error) {
	fmt.Println("Driver Debug: Driver OpenConnector")
	if driverCtx, ok := d.Driver.(driver.DriverContext); ok {
		connector, err := driverCtx.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &RecordingConnector{recorder: d.recorder, sourceName: d.sourceName, Connector: connector}, nil
	}

	return nil, errors.New("OpenConnector not implemented")
}

type RecordingConnector struct {
	Connector  driver.Connector
	recorder   *Recorder
	sourceName string
}

func (c *RecordingConnector) Connect(context context.Context) (driver.Conn, error) {
	fmt.Println("Driver Debug: Connector Connect")
	conn, err := c.Connector.Connect(context)
	if err != nil {
		return conn, err
	}
	recordingConn := &RecordingConn{Conn: conn, recorder: c.recorder, sourceName: c.sourceName}

	if _, ok := conn.(driver.ConnPrepareContext); ok {
		return &RecordingConnWithPrepareContext{recordingConn}, nil
	}

	return recordingConn, nil
}

func (c *RecordingConnector) Driver() driver.Driver {
	fmt.Println("Driver Debug: Driver Driver")
	return c.Connector.Driver()
}

type RecordingConn struct {
	Conn       driver.Conn
	recorder   *Recorder
	sourceName string
}

func (conn *RecordingConn) Prepare(query string) (driver.Stmt, error) {
	fmt.Println("Driver Debug: Conn Prepare")
	stmt, err := conn.Conn.Prepare(query)
	if err != nil {
		return stmt, err
	}

	_, isStmtQueryContext := stmt.(driver.StmtQueryContext)
	_, isStmtExecContext := stmt.(driver.StmtExecContext)
	s := &RecordingStmt{Stmt: stmt, recorder: conn.recorder, query: query, sourceName: conn.sourceName}

	if isStmtQueryContext && isStmtExecContext {
		return &RecordingStmtWithExecQueryContext{
			s,
			&RecordingStmtWithExecContext{s},
			&RecordingStmtWithQueryContext{s},
		}, nil
	}

	return s, nil
}

func (conn *RecordingConn) Close() error              { return conn.Conn.Close() }
func (conn *RecordingConn) Begin() (driver.Tx, error) { return conn.Conn.Begin() }

type RecordingConnWithQuery struct {
	*RecordingConn
}

func (conn *RecordingConnWithQuery) Query(query string, args []driver.Value) (driver.Rows, error) {
	fmt.Println("Driver Debug: Conn Query")
	var rows driver.Rows
	var err error

	if connQuery, ok := conn.Conn.(driver.Queryer); ok {
		rows, err = connQuery.Query(query, args)
		if err != nil {
			return rows, err
		}

		if conn.recorder != nil {
			recorderBody := query
			if len(args) > 0 {
				recorderBody = fmt.Sprintf("%s %+v", query, args)
			}
			conn.recorder.AddMessageRequest(MessageRequest{
				Source:    SystemUnderTestDefaultName,
				Target:    conn.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		return &RecordingRows{Rows: rows, recorder: conn.recorder, sourceName: conn.sourceName}, err
	}

	return nil, errors.New("Queryer not implemented")
}

type RecordingConnWithQueryContext struct {
	*RecordingConn
}

func (conn *RecordingConnWithQueryContext) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	fmt.Println("Driver Debug: Conn QueryContext")
	var rows driver.Rows
	var err error

	if connQueryCtx, ok := conn.Conn.(driver.QueryerContext); ok {
		rows, err = connQueryCtx.QueryContext(ctx, query, args)
		if err != nil {
			return rows, err
		}
	}

	if conn.recorder != nil {
		recorderBody := query
		if len(args) > 0 {
			convertedArgs, convertErr := namedValueToValue(args)
			if convertErr != nil {
				return nil, convertErr
			}
			recorderBody = fmt.Sprintf("%s %+v", query, convertedArgs)
		}
		conn.recorder.AddMessageRequest(MessageRequest{
			Source:    SystemUnderTestDefaultName,
			Target:    conn.sourceName,
			Header:    "SQL Query",
			Body:      recorderBody,
			Timestamp: time.Now().UTC(),
		})

		return &RecordingRows{Rows: rows, recorder: conn.recorder, sourceName: conn.sourceName}, err
	}

	return nil, errors.New("QueryerContext not implemented")
}

type RecordingConnWithExec struct {
	*RecordingConn
}

func (conn *RecordingConnWithExec) Exec(query string, args []driver.Value) (driver.Result, error) {
	fmt.Println("Driver Debug: Conn Exec")
	var result driver.Result
	var err error

	if connExec, ok := conn.Conn.(driver.Execer); ok {
		result, err = connExec.Exec(query, args)
		if err != nil {
			return result, err
		}

		if conn.recorder != nil {
			recorderBody := query
			if len(args) > 0 {
				recorderBody = fmt.Sprintf("%s %+v", query, args)
			}
			conn.recorder.AddMessageRequest(MessageRequest{
				Source:    SystemUnderTestDefaultName,
				Target:    conn.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		if result != nil && conn.recorder != nil {
			rowsAffected, _ := result.RowsAffected()
			conn.recorder.AddMessageResponse(MessageResponse{
				Source:    conn.sourceName,
				Target:    SystemUnderTestDefaultName,
				Header:    "SQL Result",
				Body:      fmt.Sprintf("Affected rows: %d", rowsAffected),
				Timestamp: time.Now().UTC(),
			})
		}

		return result, err
	}

	return nil, errors.New("Execer not implemented")
}

type RecordingConnWithExecContext struct {
	*RecordingConn
}

func (conn *RecordingConnWithExecContext) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	fmt.Println("Driver Debug: Conn ExecContext")
	var result driver.Result
	var err error

	if connExecCtx, ok := conn.Conn.(driver.ExecerContext); ok {
		result, err = connExecCtx.ExecContext(ctx, query, args)
		if err != nil {
			return result, err
		}

		if conn.recorder != nil {
			recorderBody := query
			if len(args) > 0 {
				convertedArgs, convertErr := namedValueToValue(args)
				if convertErr != nil {
					return nil, convertErr
				}
				recorderBody = fmt.Sprintf("%s %+v", query, convertedArgs)
			}
			conn.recorder.AddMessageRequest(MessageRequest{
				Source:    SystemUnderTestDefaultName,
				Target:    conn.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		if result != nil && conn.recorder != nil {
			rowsAffected, _ := result.RowsAffected()
			conn.recorder.AddMessageResponse(MessageResponse{
				Source:    conn.sourceName,
				Target:    SystemUnderTestDefaultName,
				Header:    "SQL Result",
				Body:      fmt.Sprintf("Affected rows: %d", rowsAffected),
				Timestamp: time.Now().UTC(),
			})
		}

		return result, err
	}

	return nil, errors.New("ExecerContext not implemented")
}


type RecordingConnWithPrepareContext struct {
	*RecordingConn
}

func (conn *RecordingConnWithPrepareContext) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	fmt.Println("Driver Debug: Conn PrepareContext")
	var stmt driver.Stmt
	var err error

	if connPrepareCtx, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return stmt, err
		}

		_, isStmtQueryContext := stmt.(driver.StmtQueryContext)
		_, isStmtExecContext := stmt.(driver.StmtExecContext)
		s := &RecordingStmt{Stmt: stmt, recorder: conn.recorder, query: query, sourceName: conn.sourceName}

		if isStmtQueryContext && isStmtExecContext {
			return &RecordingStmtWithExecQueryContext{s, &RecordingStmtWithExecContext{s}, &RecordingStmtWithQueryContext{s}}, nil
		}

		if isStmtQueryContext {
			return &RecordingStmtWithQueryContext{s}, nil
		}

		if isStmtExecContext {
			return &RecordingStmtWithExecContext{s}, nil
		}

		return s, nil

	}

	return nil, errors.New("ConnPrepareContext not implemented")
}

type RecordingConnWithBeginTx struct {
	*RecordingConn
}

func (conn *RecordingConnWithBeginTx) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	fmt.Println("Driver Debug: Conn BeginTx")
	if connBeginTx, ok := conn.Conn.(driver.ConnBeginTx); ok {
		return connBeginTx.BeginTx(ctx, opts)
	}

	return nil, errors.New("ConnBeginTx not implemented")
}

type RecordingConnWithPing struct {
	*RecordingConn
}

func (conn *RecordingConnWithPing) Ping(ctx context.Context) error {
	fmt.Println("Driver Debug: Conn Ping")
	if connPinger, ok := conn.Conn.(driver.Pinger); ok {
		return connPinger.Ping(ctx)
	}

	return errors.New("Pinger not implemented")
}


type RecordingConnWithExecQuery struct {
	*RecordingConn
	*RecordingConnWithExec
	*RecordingConnWithQuery
}

type RecordingConnWithExecQueryPrepareContext struct {
	*RecordingConn
	*RecordingConnWithPrepareContext
	*RecordingConnWithExecContext
	*RecordingConnWithQueryContext
	*RecordingConnWithBeginTx
	*RecordingConnWithPing
}

type RecordingStmt struct {
	Stmt       driver.Stmt
	recorder   *Recorder
	sourceName string
	query      string
}

func (stmt *RecordingStmt) Close() error {
	return stmt.Stmt.Close()
}

func (stmt *RecordingStmt) NumInput() int {
	return stmt.Stmt.NumInput()
}

func (stmt *RecordingStmt) Exec(args []driver.Value) (driver.Result, error) {
	fmt.Println("Driver Debug: Stmt Exec")
	result, err := stmt.Stmt.Exec(args)
	if stmt.recorder != nil {
		recorderBody := stmt.query
		if len(args) > 0 {
			recorderBody = fmt.Sprintf("%s %+v", stmt.query, args)
		}
		stmt.recorder.AddMessageRequest(MessageRequest{
			Source:    SystemUnderTestDefaultName,
			Target:    stmt.sourceName,
			Header:    "SQL Query",
			Body:      recorderBody,
			Timestamp: time.Now().UTC(),
		})
	}

	if result != nil && stmt.recorder != nil {
		rowsAffected, _ := result.RowsAffected()
		stmt.recorder.AddMessageResponse(MessageResponse{
			Source:    stmt.sourceName,
			Target:    SystemUnderTestDefaultName,
			Header:    "SQL Result",
			Body:      fmt.Sprintf("Affected rows: %d", rowsAffected),
			Timestamp: time.Now().UTC(),
		})
	}

	return result, err
}

func (stmt *RecordingStmt) Query(args []driver.Value) (driver.Rows, error) {
	fmt.Println("Driver Debug: Stmt Query")
	rows, err := stmt.Stmt.Query(args)

	if stmt.recorder != nil {
		recorderBody := stmt.query
		if len(args) > 0 {
			recorderBody = fmt.Sprintf("%s %+v", stmt.query, args)
		}
		stmt.recorder.AddMessageRequest(MessageRequest{
			Source:    SystemUnderTestDefaultName,
			Target:    stmt.sourceName,
			Header:    "SQL Query",
			Body:      recorderBody,
			Timestamp: time.Now().UTC(),
		})
	}

	return &RecordingRows{Rows: rows, recorder: stmt.recorder, sourceName: stmt.sourceName}, err
}

type RecordingStmtWithExecContext struct {
	*RecordingStmt
}

func (stmt *RecordingStmtWithExecContext) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	fmt.Println("Driver Debug: Stmt ExecContext")
	var result driver.Result
	var err error

	if stmtExecCtx, ok := stmt.Stmt.(driver.StmtExecContext); ok {
		result, err = stmtExecCtx.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}

		if stmt.recorder != nil {
			recorderBody := stmt.query
			if len(args) > 0 {
				convertedArgs, convertErr := namedValueToValue(args)
				if convertErr != nil {
					return nil, convertErr
				}
				recorderBody = fmt.Sprintf("%s %+v", stmt.query, convertedArgs)
			}

			stmt.recorder.AddMessageRequest(MessageRequest{
				Source:    SystemUnderTestDefaultName,
				Target:    stmt.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		if result != nil && stmt.recorder != nil {
			rowsAffected, _ := result.RowsAffected()
			stmt.recorder.AddMessageResponse(MessageResponse{
				Source:    stmt.sourceName,
				Target:    SystemUnderTestDefaultName,
				Header:    "SQL Result",
				Body:      strconv.FormatInt(rowsAffected, 10),
				Timestamp: time.Now().UTC(),
			})
		}

		return result, err
	}

	return nil, errors.New("StmtExecContext not implemented")
}

type RecordingStmtWithQueryContext struct {
	*RecordingStmt
}

func (stmt *RecordingStmtWithQueryContext) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	fmt.Println("Driver Debug: Stmt QueryContext")
	var rows driver.Rows
	var err error

	if stmtQueryCtx, ok := stmt.Stmt.(driver.StmtQueryContext); ok {
		rows, err = stmtQueryCtx.QueryContext(ctx, args)
		if err != nil {
			return nil, err
		}

		if stmt.recorder != nil {
			recorderBody := stmt.query
			if len(args) > 0 {
				convertedArgs, convertErr := namedValueToValue(args)
				if convertErr != nil {
					return nil, convertErr
				}
				recorderBody = fmt.Sprintf("%s %+v", stmt.query, convertedArgs)
			}

			stmt.recorder.AddMessageRequest(MessageRequest{
				Source:    SystemUnderTestDefaultName,
				Target:    stmt.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		return &RecordingRows{Rows: rows, recorder: stmt.recorder, sourceName: stmt.sourceName}, err
	}

	return nil, errors.New("StmtQueryContext not implemented")
}

type RecordingStmtWithExecQueryContext struct {
	*RecordingStmt
	*RecordingStmtWithExecContext
	*RecordingStmtWithQueryContext
}

type RecordingRows struct {
	Rows       driver.Rows
	recorder   *Recorder
	sourceName string
	RowsFound  int
}

func (rows *RecordingRows) Columns() []string { return rows.Rows.Columns() }
func (rows *RecordingRows) Close() error {
	fmt.Println("Driver Debug: Rows Close")
	if rows.recorder != nil {
		rows.recorder.AddMessageResponse(MessageResponse{
			Source:    rows.sourceName,
			Target:    SystemUnderTestDefaultName,
			Header:    "SQL Result",
			Body:      fmt.Sprintf("Rows returned: %d", rows.RowsFound),
			Timestamp: time.Now().UTC(),
		})
	}

	return rows.Rows.Close()
}

func (rows *RecordingRows) Next(dest []driver.Value) error {
	fmt.Println("Driver Debug: Rows Next")
	err := rows.Rows.Next(dest)
	if err != io.EOF {
		rows.RowsFound++
	}

	return err
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func sqlDriverNameToDriver(driverName string) driver.Driver {
	db, _ := sql.Open(driverName, "")
	if db != nil {
		db.Close()
		return db.Driver()
	}

	return nil
}
