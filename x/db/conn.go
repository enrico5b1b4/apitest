package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/steinfletcher/apitest"
)

type RecordingConn struct {
	Conn       driver.Conn
	recorder   *apitest.Recorder
	sourceName string
}

func (conn *RecordingConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := conn.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	_, isStmtQueryContext := stmt.(driver.StmtQueryContext)
	_, isStmtExecContext := stmt.(driver.StmtExecContext)
	recordingStmt := &RecordingStmt{
		Stmt:       stmt,
		recorder:   conn.recorder,
		query:      query,
		sourceName: conn.sourceName,
	}

	if isStmtQueryContext && isStmtExecContext {
		return &RecordingStmtWithExecQueryContext{
			recordingStmt,
			&RecordingStmtWithExecContext{recordingStmt},
			&RecordingStmtWithQueryContext{recordingStmt},
		}, nil
	}

	return recordingStmt, nil
}

func (conn *RecordingConn) Close() error { return conn.Conn.Close() }

func (conn *RecordingConn) Begin() (driver.Tx, error) { return conn.Conn.Begin() }

type RecordingConnWithQuery struct {
	*RecordingConn
}

func (conn *RecordingConnWithQuery) Query(query string, args []driver.Value) (driver.Rows, error) {
	if connQuery, ok := conn.Conn.(driver.Queryer); ok {
		rows, err := connQuery.Query(query, args)
		if err != nil {
			return nil, err
		}

		if conn.recorder != nil {
			recorderBody := query
			if len(args) > 0 {
				recorderBody = fmt.Sprintf("%s %+v", query, args)
			}
			conn.recorder.AddMessageRequest(apitest.MessageRequest{
				Source:    apitest.SystemUnderTestDefaultName,
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
	if connQueryCtx, ok := conn.Conn.(driver.QueryerContext); ok {
		rows, err := connQueryCtx.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
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
			conn.recorder.AddMessageRequest(apitest.MessageRequest{
				Source:    apitest.SystemUnderTestDefaultName,
				Target:    conn.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		return &RecordingRows{Rows: rows, recorder: conn.recorder, sourceName: conn.sourceName}, err
	}

	return nil, errors.New("QueryerContext not implemented")
}

type RecordingConnWithExec struct {
	*RecordingConn
}

func (conn *RecordingConnWithExec) Exec(query string, args []driver.Value) (driver.Result, error) {
	if connExec, ok := conn.Conn.(driver.Execer); ok {
		result, err := connExec.Exec(query, args)
		if err != nil {
			return nil, err
		}

		if conn.recorder != nil {
			recorderBody := query
			if len(args) > 0 {
				recorderBody = fmt.Sprintf("%s %+v", query, args)
			}
			conn.recorder.AddMessageRequest(apitest.MessageRequest{
				Source:    apitest.SystemUnderTestDefaultName,
				Target:    conn.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		if result != nil && conn.recorder != nil {
			rowsAffected, _ := result.RowsAffected()
			conn.recorder.AddMessageResponse(apitest.MessageResponse{
				Source:    conn.sourceName,
				Target:    apitest.SystemUnderTestDefaultName,
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
	if connExecCtx, ok := conn.Conn.(driver.ExecerContext); ok {
		result, err := connExecCtx.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
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
			conn.recorder.AddMessageRequest(apitest.MessageRequest{
				Source:    apitest.SystemUnderTestDefaultName,
				Target:    conn.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		if result != nil && conn.recorder != nil {
			rowsAffected, _ := result.RowsAffected()
			conn.recorder.AddMessageResponse(apitest.MessageResponse{
				Source:    conn.sourceName,
				Target:    apitest.SystemUnderTestDefaultName,
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
	if connPrepareCtx, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		stmt, err := connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		_, isStmtQueryContext := stmt.(driver.StmtQueryContext)
		_, isStmtExecContext := stmt.(driver.StmtExecContext)
		recordingStmt := &RecordingStmt{Stmt: stmt, recorder: conn.recorder, query: query, sourceName: conn.sourceName}

		if isStmtQueryContext && isStmtExecContext {
			return &RecordingStmtWithExecQueryContext{
				recordingStmt,
				&RecordingStmtWithExecContext{recordingStmt},
				&RecordingStmtWithQueryContext{recordingStmt},
			}, nil
		}

		if isStmtQueryContext {
			return &RecordingStmtWithQueryContext{recordingStmt}, nil
		}

		if isStmtExecContext {
			return &RecordingStmtWithExecContext{recordingStmt}, nil
		}

		return recordingStmt, nil

	}

	return nil, errors.New("ConnPrepareContext not implemented")
}

type RecordingConnWithBeginTx struct {
	*RecordingConn
}

func (conn *RecordingConnWithBeginTx) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connBeginTx, ok := conn.Conn.(driver.ConnBeginTx); ok {
		return connBeginTx.BeginTx(ctx, opts)
	}

	return nil, errors.New("ConnBeginTx not implemented")
}

type RecordingConnWithPing struct {
	*RecordingConn
}

func (conn *RecordingConnWithPing) Ping(ctx context.Context) error {
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
