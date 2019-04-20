package main

import (
	"github.com/jmoiron/sqlx"
)

const dbAddr = "host=localhost port=5432 user=postgres password=postgres dbname=apitest sslmode=disable"

func DBSetup(setup func(db *sqlx.DB)) *sqlx.DB {
	db, err := sqlx.Connect("postgres", dbAddr)
	if err != nil {
		panic(err)
	}
	setup(db)
	return db
}
