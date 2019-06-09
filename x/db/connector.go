package db

import (
	"context"
	"database/sql/driver"

	"github.com/steinfletcher/apitest"
)

type RecordingConnector struct {
	Connector  driver.Connector
	recorder   *apitest.Recorder
	sourceName string
}

func (c *RecordingConnector) Connect(context context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(context)
	if err != nil {
		return nil, err
	}

	_, isConnQuery := conn.(driver.Queryer)
	_, isConnQueryCtx := conn.(driver.QueryerContext)
	_, isConnExec := conn.(driver.Execer)
	_, isConnExecCtx := conn.(driver.ExecerContext)
	_, isConnPrepareCtx := conn.(driver.ConnPrepareContext)
	recordingConn := &RecordingConn{Conn: conn, recorder: c.recorder, sourceName: c.sourceName}

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

func (c *RecordingConnector) Driver() driver.Driver { return c.Connector.Driver() }
