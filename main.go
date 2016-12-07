package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/immesys/wd"
)

var qei *QueryEngine

// /sensors/:uuid/data?from="time"&window="time"
// default 'to' is now
// default 'from' is now-1day
// default 'window' is 5 minutes
func GetData(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	if from == "" {
		from = fmt.Sprintf("%d", time.Now().Add(-10*time.Minute).UnixNano()/1000000000)
	}
	r.Body.Close()
	fromI, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("BAD 'from' parameter"))
		return
	}
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "300"
	}
	windowI, err := strconv.ParseInt(window, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("BAD 'window' parameter"))
		return
	}
	rv, err := qei.GetData(time.Unix(fromI, 0), time.Duration(windowI)*time.Second)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(rv.JsonBytes())
	r.Body.Close()
}

/*
ucberkeley/s.giles/_/i.archiver/slot/query
ucberkeley/s.giles/_/i.archiver/signal/PXiMHag-J-t9jWXceOkTFyNKGLFoXdTZKcNRZEGlPBE,queries
*/

func main() {
	qei = NewQueryEngine()
	go func() {
		for {
			wd.Kick(os.Getenv("WD_PREFIX")+".running", 300)
			time.Sleep(100)
		}
	}()
	//http.Handle("/images/", http.StripPrefix("/images", http.FileServer(http.Dir("/srv/ampimg"))))
	http.Handle("/data", http.HandlerFunc(GetData))
	if err := http.ListenAndServe(":9999", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
