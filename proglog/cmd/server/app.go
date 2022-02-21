package main

import (
	"log"
	"os"

	"github.com/mstreet3/proglog/internal/server"
)

var addr string = ":8080"

func main() {
	logger := log.New(os.Stdout, "http://", log.LstdFlags)
	logger.Println("Server is starting...")
	srv := server.NewHTTPServer(addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}
