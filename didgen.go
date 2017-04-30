package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"Didgen/config"
	"Didgen/db"
	log "Didgen/logger_seelog"
	"Didgen/server"
	_ "net/http/pprof"
)

func init() {
	configPath := "./configuration.yml"

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Init Config error: (%s) does not exist!\n", configPath)
		os.Exit(1)
	} else {
		err = config.Init(configPath)
		if err != nil {
			fmt.Printf("Init Config failed: (%s), error: %v!\n", configPath, err)
			os.Exit(1)
		}
	}

	_, err := log.NewLogger("main", config.Config.LogPath, "didgen.log", config.Config.LogLevel, "size", "20971520", "5", true)
	if err != nil {
		fmt.Printf(fmt.Sprintf("Init logger error: %s\n", err))
		os.Exit(1)
	}

	db.InitConfig()

	threads := runtime.GOMAXPROCS(config.Config.Threads)
	log.Info(fmt.Sprintf("Server with threads: %d", threads))
	log.Info(fmt.Sprintf("Server config path: %s", configPath))
	log.Info(fmt.Sprintf("Server log path: %s", config.Config.LogPath))
	log.Info(fmt.Sprintf("Server log level: %s", config.Config.LogLevel))
	log.Info(fmt.Sprintf("Server host: %s", config.Config.ServerHost))
	log.Info(fmt.Sprintf("Server port: %s", config.Config.ServerPort))
	log.Info(fmt.Sprintf("Server data path: %s", config.Config.DataPath))
	log.Info(fmt.Sprintf("Server batch_size: %d", config.Config.BatchSize))
	log.Info(fmt.Sprintf("Server nodes: %v", config.Config.Nodes))
}

func main() {
	log.Info("Start Service")
	db.InitData()
	var s *server.Server
	s, err := server.NewServer(config.Config.ServerHost, config.Config.ServerPort)
	if err != nil {
		log.Error(fmt.Sprintf("Create Server, error: %v", err))
		s.Close()
		os.Exit(1)
	}

	err = s.Init()
	if err != nil {
		log.Error(fmt.Sprintf("Init Server error: %v", err))
		s.Close()
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Info(fmt.Sprintf("Got signal: %v", sig))
		s.Close()
	}()
	log.Info(fmt.Sprintf("Server running!"))
	s.Serve()

	log.Info("Close Service")
	os.Exit(0)
}
