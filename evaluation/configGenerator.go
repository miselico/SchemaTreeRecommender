package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/lgleim/SchemaTreeRecommender/configuration"
)

type creater struct {
	Conds        []string // List of conditions to evaluate
	Merger       []string // List of mergers to evaluate
	Splitter     []string // List of splitters to evaluate
	Steps        []string // List of stepfunctions to evaluate
	MaxThreshold int      // Threshold for condition
	MaxParallel  int      // Maximal parallel executions for backoff DeleteLowFrequency
	MaxFloat     float32  // Maximal float value for condition TooUnlikelyRecommendations
}

func readCreaterConfig(name *string) (conf *creater, err error) {
	var c creater
	file, err := ioutil.ReadFile(*name)
	if err != nil {
		return
	}
	err = json.Unmarshal(file, &c)
	conf = &c
	return
}

// Creates a bunch of config files in ./configs
func createConfigFiles(creater *string) (err error) {

	createrConfig, err := readCreaterConfig(creater)

	fallbackLayer := configuration.Layer{"always", "standard", 0, 0.0, "", "", "", 0}
	backoffLayers := make([]configuration.Layer, 0, 0)

	// create a bunch of layers
	for thresh := 1; thresh <= createrConfig.MaxThreshold; thresh++ {
		for _, con := range createrConfig.Conds {

			//split property backoff
			for _, m := range createrConfig.Merger {
				for _, s := range createrConfig.Splitter {
					if con == "tooUnlikelyRecommendationsCondition" {
						fthresh := (float32(thresh) / float32(createrConfig.MaxThreshold)) * createrConfig.MaxFloat
						l := configuration.Layer{con, "splitProperty", thresh, fthresh, m, s, "", 0}
						backoffLayers = append(backoffLayers, l)

					} else {
						l := configuration.Layer{con, "splitProperty", thresh, 0.0, m, s, "", 0}
						backoffLayers = append(backoffLayers, l)
					}
				}
			}
			//delete lowfrequencyitem backoff
			for parallel := 1; parallel <= createrConfig.MaxParallel; parallel++ {
				for _, s := range createrConfig.Steps {
					if con == "tooUnlikelyRecommendationsCondition" {
						fthresh := (float32(thresh) / float32(createrConfig.MaxThreshold)) * createrConfig.MaxFloat
						l := configuration.Layer{con, "deleteLowFrequency", thresh, fthresh, "", "", s, parallel}
						backoffLayers = append(backoffLayers, l)

					} else {
						l := configuration.Layer{con, "deleteLowFrequency", thresh, 0.00, "", "", s, parallel}
						backoffLayers = append(backoffLayers, l)
					}
				}
			}
		}
	}

	// create config files from backoff layers
	for i, l := range backoffLayers {
		c := configuration.Configuration{"../testdata/10M.nt_1in2_test.gz", []configuration.Layer{l, fallbackLayer}}
		err = writeConfigFile(&c, fmt.Sprintf("./configs/config_%v.json", i))
		if err != nil {
			log.Fatal("could not write config file ", err)
			return
		}
	}
	return
}

// write config file ./configs/<name>.json to Configuration struct
func writeConfigFile(config *configuration.Configuration, name string) (err error) {
	// encode/marshal directly with json because marshal is not implemented in viper
	file, err := json.Marshal(*config)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(name, file, 0777)
	return
}
