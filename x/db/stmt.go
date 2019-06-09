package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/steinfletcher/apitest"
)

type RecordingStmt struct {
	Stmt       driver.Stmt
	recorder   *apitest.Recorder
	sourceName string
	query      string
}

func (stmt *RecordingStmt) Close() error { return stmt.Stmt.Close() }

func (stmt *RecordingStmt) NumInput() int { return stmt.Stmt.NumInput() }

func (stmt *RecordingStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := stmt.Stmt.Exec(args)
	if stmt.recorder != nil {
		recorderBody := stmt.query
		if len(args) > 0 {
			recorderBody = fmt.Sprintf("%s %+v", stmt.query, args)
		}
		stmt.recorder.AddMessageRequest(apitest.MessageRequest{
			Source:    apitest.SystemUnderTestDefaultName,
			Target:    stmt.sourceName,
			Header:    "SQL Query",
			Body:      recorderBody,
			Timestamp: time.Now().UTC(),
		})
	}

	if result != nil && stmt.recorder != nil {
		rowsAffected, _ := result.RowsAffected()
		stmt.recorder.AddMessageResponse(apitest.MessageResponse{
			Source:    stmt.sourceName,
			Target:    apitest.SystemUnderTestDefaultName,
			Header:    "SQL Result",
			Body:      fmt.Sprintf("Affected rows: %d", rowsAffected),
			Timestamp: time.Now().UTC(),
		})
	}

	return result, err
}

func (stmt *RecordingStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := stmt.Stmt.Query(args)

	if stmt.recorder != nil {
		recorderBody := stmt.query
		if len(args) > 0 {
			recorderBody = fmt.Sprintf("%s %+v", stmt.query, args)
		}
		stmt.recorder.AddMessageRequest(apitest.MessageRequest{
			Source:    apitest.SystemUnderTestDefaultName,
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
	if stmtExecCtx, ok := stmt.Stmt.(driver.StmtExecContext); ok {
		result, err := stmtExecCtx.ExecContext(ctx, args)
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

			stmt.recorder.AddMessageRequest(apitest.MessageRequest{
				Source:    apitest.SystemUnderTestDefaultName,
				Target:    stmt.sourceName,
				Header:    "SQL Query",
				Body:      recorderBody,
				Timestamp: time.Now().UTC(),
			})
		}

		if result != nil && stmt.recorder != nil {
			rowsAffected, _ := result.RowsAffected()
			stmt.recorder.AddMessageResponse(apitest.MessageResponse{
				Source:    stmt.sourceName,
				Target:    apitest.SystemUnderTestDefaultName,
				Header:    "SQL Result",
				Body:      fmt.Sprintf("Affected rows: %d", rowsAffected),
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
	if stmtQueryCtx, ok := stmt.Stmt.(driver.StmtQueryContext); ok {
		rows, err := stmtQueryCtx.QueryContext(ctx, args)
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

			stmt.recorder.AddMessageRequest(apitest.MessageRequest{
				Source:    apitest.SystemUnderTestDefaultName,
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
