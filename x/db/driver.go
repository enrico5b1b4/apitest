package db

import (
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/steinfletcher/apitest"
)

// WrapWithRecorder wraps an existing driver with a Recorder
func WrapWithRecorder(driverName string, recorder *apitest.Recorder) driver.Driver {
	sqlDriver := sqlDriverNameToDriver(driverName)
	recordingDriver := &RecordingDriver{
		sourceName: driverName,
		Driver:     sqlDriver,
		recorder:   recorder,
	}

	if _, ok := sqlDriver.(driver.DriverContext); ok {
		return &RecordingDriverContext{recordingDriver}
	}

	return recordingDriver
}

type RecordingDriver struct {
	Driver     driver.Driver
	recorder   *apitest.Recorder
	sourceName string
}

func (d *RecordingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
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
	if driverCtx, ok := d.Driver.(driver.DriverContext); ok {
		connector, err := driverCtx.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &RecordingConnector{recorder: d.recorder, sourceName: d.sourceName, Connector: connector}, nil
	}

	return nil, errors.New("OpenConnector not implemented")
}

// see https://golang.org/src/database/sql/ctxutil.go
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

// sqlDriverNameToDriver opens a dummy connection to get a driver
func sqlDriverNameToDriver(driverName string) driver.Driver {
	db, _ := sql.Open(driverName, "")
	if db != nil {
		db.Close()
		return db.Driver()
	}

	return nil
}
