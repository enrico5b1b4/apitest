package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sqlx.Connect("postgres", dbAddr)
	if err != nil {
		panic(err)
	}

	newApp(db).start()
}

type App struct {
	Router *mux.Router
	DB     *sqlx.DB
}

func newApp(db *sqlx.DB) *App {
	router := mux.NewRouter()
	router.HandleFunc("/user", getUser(db)).Methods("GET")
	return &App{Router: router, DB: db}
}

func (a *App) start() {
	log.Fatal(http.ListenAndServe(":8888", a.Router))
}

func getUser(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("CALLED getUser")
		var user User
		get(fmt.Sprintf("http://users/api/user?id=%s", r.URL.Query()["name"]), &user)

		var isContactable bool
		err := db.Get(&isContactable, "SELECT is_contactable from users where username=$1 AND is_contactable=$2 LIMIT 1", user.Name, true)
		if err != nil {
			panic(err)
		}

		result := []string{}
		errSelect := db.Select(&result, "SELECT username from users where is_contactable=$1", true)
		if errSelect != nil {
			panic(errSelect)
		}

		var id int
		err = db.Get(&id, "SELECT count(*) FROM users")

		var names []string
		err = db.Select(&names, "SELECT username FROM users LIMIT 10")

		tx := db.MustBegin()
		tx.Exec("DELETE FROM users WHERE username=$1", user.Name)
		tx.Exec("DELETE FROM users")
		tx.Commit()

		response := UserResponse1{
			Name:          user.Name,
			IsContactable: isContactable,
		}

		bytes, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json")
		w.Write(bytes)
		w.WriteHeader(http.StatusOK)
	}
}

type User struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

type UserResponse1 struct {
	Name          string `json:"name"`
	IsContactable bool   `json:"is_contactable"`
}

func get(path string, response interface{}) {
	res, err := http.Get(path)
	if err != nil {
		panic(err)
	}

	if !(res.StatusCode >= http.StatusOK && res.StatusCode < 400) {
		panic(fmt.Sprintf("unexpected status code=%d", res.StatusCode))
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(bytes, response)
	if err != nil {
		panic(err)
	}
}
