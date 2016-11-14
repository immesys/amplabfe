package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/bmizerany/pat"
	"github.com/stretchr/graceful"
)

type FEHandler struct {
	mu sync.Mutex
}

//List sensors
func (fe *FEHandler) ListSensors(w http.ResponseWriter, r *http.Request) {

}

// /sensors/:uuid/data?from="time"&to="time"&window="time"
// default 'to' is now
// default 'from' is now-1day
// default 'window' is 5 minutes
func (fe *FEHandler) GetData(w http.ResponseWriter, r *http.Request) {

}

// /sensors/:uuid/metadata
func (fe *FEHandler) GetMetadata(w http.ResponseWriter, r *http.Request) {

}

func main() {
	fe := &FEHandler{}

	go MaintainState()

	mux := pat.New()

	mux.Get("/list", http.HandlerFunc(fe.ListSensors))
	mux.Get("/data/:uuid", http.HandlerFunc(fe.GetData))
	mux.Get("/metadata/:uuid", http.HandlerFunc(fe.GetMetadata))
	graceful.Run("0.0.0.0:8080", 10*time.Second, mux)
}
