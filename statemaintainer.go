package main

import (
	"fmt"
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

	TempMin   float64 `json:"temp_min"`
	TempMax   float64 `json:"temp_max"`
	TempMean  float64 `json:"temp_mean"`
	TempCount int64   `json:"temp_count"`

	HumidityMin   float64 `json:"humidity_min"`
	HumidityMean  float64 `json:"humidity_mean"`
	HumidityMax   float64 `json:"humidity_max"`
	HumidityCount int64   `json:"humidity_count"`

	PresenceMean  float64 `json:"presence_mean"`
	PresenceCount int64   `json:"presence_count"`

	LuxMin   float64 `json:"lux_min"`
	LuxMean  float64 `json:"lux_mean"`
	LuxMax   float64 `json:"lux_max"`
	LuxCount int64   `json:"lux_count"`
}

//This is a given state object
type StateMap struct {
	WindowStart  int64                   `json:"window_start"`
	WindowLength int64                   `json:"window_length"`
	Data         map[string]*SensorState `json:"data"`
}

var stateMutex sync.RWMutex
var state_5min []StateMap

func MaintainState() {
	bwcl := bw2bind.ConnectOrExit("")
	vk := bwcl.SetEntityFromEnvironOrExit()
	pd := client.NewPundatClient(bwcl, vk, "ucberkeley")
	for {
		ts, md, ch, err := pd.Query(`select * where has buildingrcoords and path like "TwMwEkCRO-Cg3m1RBlgCQUeJPwRttSiLHppLhuHUDeU=/sensors/s.hamilton/.*/i.temperature/signal/temperature"`)
		if err != nil {
			fmt.Printf("Pundat error: %v\n", err)
		}
		fmt.Printf("metadata: %v\n", md)
		_ = ts
		_ = ch
		time.Sleep(5 * time.Second)

	}
}
