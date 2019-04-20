package main

import (
	"github.com/jmoiron/sqlx"
)

const dbAddr = "./foo.db"

func DBSetup(setup func(db *sqlx.DB)) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", dbAddr)
	if err != nil {
		panic(err)
	}
	setup(db)
	return db
}
