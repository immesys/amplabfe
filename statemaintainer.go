package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gtfierro/PunDat/client"
	"github.com/gtfierro/pundat/archiver"
	"github.com/immesys/wd"
	"gopkg.in/immesys/bw2bind.v5"
)

//SensorState represents an ideal hamilton sensor. Most sensors will have
//missing data for one or more of these fields. The "count" field should
//be used to determine if the readings are populated or not.
type SensorState struct {
	ID string

	RCoordX float64 `json:"rcoord_x"`
	RCoordY float64 `json:"rcoord_y"`

	TempMin   float64 `json:"temp_min"`
	TempMax   float64 `json:"temp_max"`
	TempMean  float64 `json:"temp_mean"`
	TempCount uint64  `json:"temp_count"`
	tempUUID  string  `json:"-"`

	HumidityMin   float64 `json:"humidity_min"`
	HumidityMean  float64 `json:"humidity_mean"`
	HumidityMax   float64 `json:"humidity_max"`
	HumidityCount uint64  `json:"humidity_count"`
	humidityUUID  string  `json:"-"`

	PresenceMean  float64 `json:"presence_mean"`
	PresenceCount uint64  `json:"presence_count"`
	presenceUUID  string  `json:"-"`

	LuxMin   float64 `json:"lux_min"`
	LuxMean  float64 `json:"lux_mean"`
	LuxMax   float64 `json:"lux_max"`
	LuxCount uint64  `json:"lux_count"`
	luxUUID  string  `json:"-"`
}

//This is a given state object
type StateMap struct {
	WindowStart  int64                   `json:"window_start"`
	WindowLength uint64                  `json:"window_length"`
	Data         map[string]*SensorState `json:"data"`
}

type QueryKey struct {
	Start  time.Time
	Length time.Duration
}

var cacheMutex sync.RWMutex
var cache map[QueryKey]*StateMap

func (s *StateMap) JsonBytes() []byte {
	b, _ := json.MarshalIndent(s, " ", " ")
	return b
}

type QueryEngine struct {
	bwcl *bw2bind.BW2Client
	vk   string
	pd   *client.PundatClient
	pdm  *client.PundatClient
}

func NewQueryEngine() *QueryEngine {
	cache = make(map[QueryKey]*StateMap)
	rv := QueryEngine{}
	rv.bwcl = bw2bind.ConnectOrExit("")
	rv.vk = rv.bwcl.SetEntityFromEnvironOrExit()
	rv.pd = client.NewPundatClient(rv.bwcl, rv.vk, "ucberkeley")
	rv.pdm = client.NewPundatClient(rv.bwcl, rv.vk, "scratch.ns")
	return &rv
}

func (qe *QueryEngine) GetData(start time.Time, length time.Duration) (*StateMap, error) {
	key := QueryKey{start, length}
	cacheMutex.Lock()
	crv, ok := cache[key]
	cacheMutex.Unlock()
	if ok {
		return crv, nil
	}
	res := StateMap{}
	res.Data = make(map[string]*SensorState)
	res.WindowStart = start.UnixNano()
	res.WindowLength = uint64(length.Nanoseconds())
	qry1 := `select * where has rcoords and path like "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/operative"`
	md, _, _, err := qe.pdm.Query(qry1)
	if err != nil {
		return nil, fmt.Errorf("archiver error: %v\n", err)
	}

	fmt.Printf("metadata returned %d results\n", len(md.Data))
	if len(md.Data) > 0 {
		wd.Kick(os.Getenv("WD_PREFIX")+".archiver.mdq", 600)
	} else {
		wd.Fault(os.Getenv("WD_PREFIX")+".archiver.mdq", "no metadata results")
	}
	//for _, m := range md.Data {
	//	fmt.Printf("%s (%s)\n", m.Path, m.Metadata["_name"])
	//}
	for _, m := range md.Data {
		coordsi, ok := m.Metadata["rcoords"]
		if !ok {
			fmt.Println("Warning: no rcoords")
			continue
		}
		coords, ok := coordsi.(string)
		if !ok {
			fmt.Println("odd rcoords type")
			continue
		}
		parts := strings.Split(coords, ",")
		if len(parts) != 2 {
			fmt.Println("Warning: bad rcoords")
			continue
		}
		xcoord, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			fmt.Println("Bad X coord")
			continue
		}
		ycoord, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Println("Bad Y coord")
			continue
		}
		pathel := strings.Split(m.Path, "/")
		if len(pathel) != 7 {
			fmt.Println("bad path")
			continue
		}
		id := pathel[3]
		ex, ok := res.Data[id]
		if !ok {
			ex = &SensorState{
				ID:      id,
				RCoordX: float64(xcoord),
				RCoordY: float64(ycoord),
			}
			res.Data[id] = ex
		}
		switch m.Metadata["_name"] {
		case "air_temp":
			ex.tempUUID = m.UUID
		case "lux":
			ex.luxUUID = m.UUID
		case "air_rh":
			ex.humidityUUID = m.UUID
		case "presence":
			ex.presenceUUID = m.UUID
		}
	}
	qry := `select window(%dns) data in (%dns,%dns) where path like "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/operative"`

	sqry := fmt.Sprintf(qry, length.Nanoseconds(), start.UnixNano(), start.UnixNano()+length.Nanoseconds())
	fmt.Printf("sqry is: %s\n", sqry)
	_, ts, _, err := qe.pd.Query(sqry)
	if err != nil {
		return nil, fmt.Errorf("archiver error (phase 2): %v\n", err)
	}
	fmt.Printf("data returned %d streams\n", len(ts.Stats))
	if len(ts.Stats) > 0 {
		wd.Kick(os.Getenv("WD_PREFIX")+".archiver.dat", 600)
	} else {
		wd.Fault(os.Getenv("WD_PREFIX")+".archiver.dat", "got zero results")
	}
	getStat := func(uuid string) *archiver.Statistics {
		for _, el := range ts.Stats {
			if el.UUID == uuid {
				return &el
			}
		}
		return nil
	}
	ntemp := 0
	nhum := 0

	for _, sso := range res.Data {
		ss := sso

		if ss.tempUUID != "" {
			el := getStat(ss.tempUUID)
			if el != nil && len(el.Mean) >= 1 {
				ntemp++
				ss.TempMin = el.Min[0]
				ss.TempMean = el.Mean[0]
				ss.TempMax = el.Max[0]
				ss.TempCount = el.Count[0]
			}
		}
		if ss.humidityUUID != "" {
			el := getStat(ss.humidityUUID)
			if el != nil && len(el.Mean) >= 1 {
				nhum++
				ss.HumidityMin = el.Min[0]
				ss.HumidityMean = el.Mean[0]
				ss.HumidityMax = el.Max[0]
				ss.HumidityCount = el.Count[0]
			}
		}
		if ss.presenceUUID != "" {
			el := getStat(ss.presenceUUID)
			if el != nil && len(el.Mean) >= 1 {
				ss.PresenceMean = el.Mean[0]
				ss.PresenceCount = el.Count[0]
			}
		}
		if ss.luxUUID != "" {
			el := getStat(ss.luxUUID)
			if el != nil && len(el.Mean) >= 1 {
				ss.LuxMin = el.Min[0]
				ss.LuxMean = el.Mean[0]
				ss.LuxMax = el.Max[0]
				ss.LuxCount = el.Count[0]
			}
		}
	}
	fmt.Printf("got ntemp=%d and nhum=%d\n", ntemp, nhum)
	if len(cache) > 1000 {
		cacheMutex.Lock()
		i := 0
		for k, _ := range cache {
			delete(cache, k)
			i++
			if i == 50 {
				break
			}
		}
		cacheMutex.Unlock()
	}
	if start.Before(time.Now().Add(-(length + 3*time.Minute))) {
		fmt.Println("caching")
		cacheMutex.Lock()
		cache[key] = &res
		cacheMutex.Unlock()
	} else {
		fmt.Println("no cache -- too recent")
	}

	return &res, nil
}

//
// func MaintainState() {
// 	bwcl := bw2bind.ConnectOrExit("")
// 	vk := bwcl.SetEntityFromEnvironOrExit()
// 	pd := client.NewPundatClient(bwcl, vk, "ucberkeley")
// 	for {
// 		intermediate := make(map[string]*SensorState)
// 		qry1 := `select * where has rcoords and building like "Soda Hall" and path like "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/operative"`
// 		fmt.Printf("executing %s\n", qry1)
// 		md, ts, _, err := pd.Query(qry1)
// 		if err != nil {
// 			fmt.Printf("Pundat error: %v\n", err)
// 		}
// 		// fmt.Printf("1.metadata: %v\n", md)
// 		// fmt.Printf("1.ts: %v\n", ts)
// 		// fmt.Printf("1.ch: %v\n", ch)
// 		for _, m := range md.Data {
// 			coordsi, ok := m.Metadata["rcoords"]
// 			if !ok {
// 				fmt.Println("Warning: no rcoords")
// 				continue
// 			}
// 			coords, ok := coordsi.(string)
// 			if !ok {
// 				fmt.Println("odd rcoords type")
// 				continue
// 			}
// 			parts := strings.Split(coords, ",")
// 			if len(parts) != 2 {
// 				fmt.Println("Warning: bad rcoords")
// 				continue
// 			}
// 			xcoord, err := strconv.ParseFloat(parts[0], 64)
// 			if err != nil {
// 				fmt.Println("Bad X coord")
// 				continue
// 			}
// 			ycoord, err := strconv.ParseFloat(parts[1], 64)
// 			if err != nil {
// 				fmt.Println("Bad Y coord")
// 				continue
// 			}
// 			pathel := strings.Split(m.Path, "/")
// 			if len(pathel) != 7 {
// 				fmt.Println("bad path")
// 				continue
// 			}
// 			id := pathel[3]
// 			intermediate[m.UUID] = &SensorState{
// 				ID:      id,
// 				RCoordX: float64(xcoord),
// 				RCoordY: float64(ycoord),
// 			}
// 		}
// 		now := time.Now().UnixNano()
// 		wlen := uint64(15 * 60 * 1000 * 1000 * 1000)
// 		then := now - int64(wlen)
// 		// where has buildingrcoords and path like "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/temperature"
// 		URL := "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/operative"
// 		qry := `select window(15min) data in (%dns,%dns) where path like "%s" and has rcoords and building like "Soda Hall"`
// 		fmt.Println("executing ", fmt.Sprintf(qry, then, now, URL))
// 		md, ts, _, err = pd.Query(fmt.Sprintf(qry, then, now, URL))
// 		// fmt.Printf("md is %v\n", md)
// 		// fmt.Printf("ts is %v\n", ts)
// 		// fmt.Printf("ch is %v\n", ch)
// 		// fmt.Printf("err is %v\n", err)
// 		for _, el := range ts.Stats {
// 			item, ok := intermediate[el.UUID]
// 			if !ok {
// 				fmt.Println("odd, could not find item")
// 				continue
// 			}
// 			item.TempMin = el.Min[0]
// 			item.TempMean = el.Mean[0]
// 			item.TempMax = el.Max[0]
// 			item.TempCount = el.Count[0]
// 		}
// 		sm := &StateMap{
// 			WindowStart:  then / 1000000, //ms
// 			WindowLength: wlen / 1000000,
// 			Data:         make(map[string]*SensorState),
// 		}
// 		for _, el := range intermediate {
// 			sm.Data[el.ID] = el
// 		}
// 		fmt.Printf("State updated: \n")
// 		b, _ := json.MarshalIndent(sm, " ", " ")
//
// 		stateMutex.Lock()
// 		state = sm
// 		stateString = b
// 		stateMutex.Unlock()
// 		fmt.Println(string(b))
// 		//	fmt.Printf("complete: %#v\n", sm)
// 		//Now query the temperature for the past 5 minutes
// 		time.Sleep(1 * time.Minute)
//
// 	}
//}
