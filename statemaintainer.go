package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gtfierro/PunDat/client"
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

var stateMutex sync.RWMutex
var state *StateMap
var stateString []byte

func (s *StateMap) JsonBytes() []byte {
	b, _ := json.MarshalIndent(s, " ", " ")
	return b
}

type QueryEngine struct {
	bwcl *bw2bind.BW2Client
	vk   string
	pd   *client.PundatClient
}

func NewQueryEngine() *QueryEngine {
	rv := QueryEngine{}
	rv.bwcl = bw2bind.ConnectOrExit("")
	rv.vk = rv.bwcl.SetEntityFromEnvironOrExit()
	rv.pd = client.NewPundatClient(rv.bwcl, rv.vk, "ucberkeley")
	return &rv
}

func (qe *QueryEngine) GetData(start time.Time, length time.Duration) (*StateMap, error) {
	res := StateMap{}
	res.Data = make(map[string]*SensorState)
	res.WindowStart = start.UnixNano()
	res.WindowLength = uint64(length.Nanoseconds())
	qry1 := `select * where has rcoords and building like "Soda Hall" and path like "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/operative"`
	md, _, _, err := qe.pd.Query(qry1)
	if err != nil {
		return nil, fmt.Errorf("archiver error: %v\n", err)
	}
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
	//PAR wg := sync.WaitGroup{}
	//PAR wg.Add(len(res.Data))
	for _, sso := range res.Data {
		ss := sso
		//PAR go func() (interface{}, interface{}) {
		qry := `select window(%dns) data in (%dns,%dns) where uuid="%s"`
		if ss.tempUUID != "" {
			sqry := fmt.Sprintf(qry, length.Nanoseconds(), start.UnixNano(), start.UnixNano()+length.Nanoseconds(), ss.tempUUID)
			_, ts, _, err := qe.pd.Query(sqry)
			if err != nil {
				return nil, fmt.Errorf("archiver error (phase 2): %v\n", err)
			}
			el := ts.Stats[0]
			ss.TempMin = el.Min[0]
			ss.TempMean = el.Mean[0]
			ss.TempMax = el.Max[0]
			ss.TempCount = el.Count[0]
		}
		if ss.humidityUUID != "" {
			sqry := fmt.Sprintf(qry, length.Nanoseconds(), start.UnixNano(), start.UnixNano()+length.Nanoseconds(), ss.humidityUUID)
			_, ts, _, err := qe.pd.Query(sqry)
			if err != nil {
				return nil, fmt.Errorf("archiver error (phase 2): %v\n", err)
			}
			el := ts.Stats[0]
			ss.HumidityMin = el.Min[0]
			ss.HumidityMean = el.Mean[0]
			ss.HumidityMax = el.Max[0]
			ss.HumidityCount = el.Count[0]
		}
		if ss.presenceUUID != "" {
			sqry := fmt.Sprintf(qry, length.Nanoseconds(), start.UnixNano(), start.UnixNano()+length.Nanoseconds(), ss.presenceUUID)
			_, ts, _, err := qe.pd.Query(sqry)
			if err != nil {
				return nil, fmt.Errorf("archiver error (phase 2): %v\n", err)
			}
			el := ts.Stats[0]
			ss.PresenceMean = el.Mean[0]
			ss.PresenceCount = el.Count[0]
			//fmt.Printf("pd")
		}
		if ss.luxUUID != "" {
			sqry := fmt.Sprintf(qry, length.Nanoseconds(), start.UnixNano(), start.UnixNano()+length.Nanoseconds(), ss.luxUUID)
			_, ts, _, err := qe.pd.Query(sqry)
			if err != nil {
				return nil, fmt.Errorf("archiver error (phase 2): %v\n", err)
			}
			el := ts.Stats[0]
			ss.LuxMin = el.Min[0]
			ss.LuxMean = el.Mean[0]
			ss.LuxMax = el.Max[0]
			ss.LuxCount = el.Count[0]
		}
		//PAR wg.Done()
		//PAR return nil, nil
		//PAR }()
	}
	//PAR wg.Wait()
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
