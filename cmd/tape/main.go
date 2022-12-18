package main

import (
	"fmt"
	"os"

	"github.com/osdi23p228/tape/pkg/infra"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	fullCmd string
)

var (
	app        = kingpin.New("tape", "A performance measurement tool for Hyperledger Fabric")
	run        = app.Command("run", "Run this program").Default()
	version    = app.Command("version", "Show version information")
	configFile = run.Flag("config", "Path of config file").Required().Short('c').String()
)

func setLogLevel(logger *log.Logger) {
	logger.SetLevel(log.InfoLevel)
	if value, ok := os.LookupEnv("TAPE_LOGLEVEL"); ok {
		if level, err := log.ParseLevel(value); err == nil {
			logger.SetLevel(level)
		}
	}
}

func getLogger() *log.Logger {
	logger := log.New()
	setLogLevel(logger)
	return logger
}

func getConfig() *infra.Config {
	config, err := infra.LoadConfigFromFile(*configFile)
	if err != nil {
		log.Panicf("Fail to load config: %v\n", err)
	}
	return config
}

func main() {
	var err error
	logger := getLogger()

	fullCmd = kingpin.MustParse(app.Parse(os.Args[1:]))
	switch fullCmd {
	case run.FullCommand():
		config := getConfig()
		infra.Process(config, logger)
	case version.FullCommand():
		fmt.Printf(infra.GetVersionInfo())
	default:
		err = errors.Errorf("Invalid command: %s", fullCmd)
	}

	if err != nil {
		logger.Panicln(err)
	}
	os.Exit(0)
}
