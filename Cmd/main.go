package main

import (
	"log"

	server "github.com/MQasim1218/prolog/Internal/Server"
)

func main() {
	srv := server.NewHttpSrvr(":8080")
	log.Fatal(srv.ListenAndServe())
}
