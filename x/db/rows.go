package db

import (
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/steinfletcher/apitest"
)

type RecordingRows struct {
	Rows       driver.Rows
	recorder   *apitest.Recorder
	sourceName string
	RowsFound  int
}

func (rows *RecordingRows) Columns() []string { return rows.Rows.Columns() }

func (rows *RecordingRows) Close() error {
	if rows.recorder != nil {
		rows.recorder.AddMessageResponse(apitest.MessageResponse{
			Source:    rows.sourceName,
			Target:    apitest.SystemUnderTestDefaultName,
			Header:    "SQL Result",
			Body:      fmt.Sprintf("Rows returned: %d", rows.RowsFound),
			Timestamp: time.Now().UTC(),
		})
	}

	return rows.Rows.Close()
}

func (rows *RecordingRows) Next(dest []driver.Value) error {
	err := rows.Rows.Next(dest)
	if err != io.EOF {
		rows.RowsFound++
	}

	return err
}
