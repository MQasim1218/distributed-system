package server

/*
	Our API has two endpoints:
	1. Produce for writing to the log and,
	2. Consume for reading from the log.
*/

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type (
	httpServerLog struct {
		log *Log
	}

	// A produce request contains the record that the caller of our API wants
	// appended to the log
	ProductReq struct {
		Record Record `json:"record"`
	}

	// produce response tells the caller what offset the
	// log stored the records under
	ProductRes struct {
		Offset uint64 `json:"offset"`
	}

	// A consume request specifies which records the
	// caller of our API wants to read
	ConsumeReq struct {
		Offset uint64 `json:"offset"`
	}

	// consume response to send back those records to the caller
	ConsumeRes struct {
		Record Record `json:"record"`
	}
)

func (srv *httpServerLog) handleProduce(w http.ResponseWriter, r *http.Request) {

	log.Println("We are in the Handle Produce Function")

	var req ProductReq = ProductReq{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Println(req)

	off, err := srv.log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProductRes{Offset: off}
	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (srv *httpServerLog) handleConsume(w http.ResponseWriter, r *http.Request) {
	// ConsumeReq holds the `Index` of the Record that the Caller wants to read
	var req = ConsumeReq{}

	// Get Index, save to Req object
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := srv.log.Read(req.Offset)
	if err != nil {

		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func newHttpSrvr() *httpServerLog {
	return &httpServerLog{
		log: NewLog(),
	}
}

func NewHttpSrvr(addr string) *http.Server {
	httpsrvlogs := newHttpSrvr()
	r := mux.NewRouter()

	log.Println("Here in this Function!!")

	r.HandleFunc("/", httpsrvlogs.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrvlogs.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
