package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"Didgen/config"
	"Didgen/db"
	logger "Didgen/logger_seelog"
	"Didgen/server"
	"github.com/cihub/seelog"
	_ "net/http/pprof"
)

var Log seelog.LoggerInterface

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

	Logger, err := logger.NewLogger("main", config.Config.LogPath, "didgen.log", config.Config.LogLevel, "size", "20971520", "5", true)
	if err != nil {
		fmt.Printf(fmt.Sprintf("Init logger error: %s\n", err))
		os.Exit(1)
	}
	Log = *Logger

	db.InitLog()
	server.InitLog()

	threads := runtime.GOMAXPROCS(config.Config.Threads)
	Log.Info(fmt.Sprintf("Server with threads: %d", threads))
	Log.Info(fmt.Sprintf("Server config path: %s", configPath))
	Log.Info(fmt.Sprintf("Server log path: %s", config.Config.LogPath))
	Log.Info(fmt.Sprintf("Server log level: %s", config.Config.LogLevel))
	Log.Info(fmt.Sprintf("Server host: %s", config.Config.ServerHost))
	Log.Info(fmt.Sprintf("Server port: %s", config.Config.ServerPort))
	Log.Info(fmt.Sprintf("Server data path: %s", config.Config.DataPath))
	Log.Info(fmt.Sprintf("Server batch_size: %d", config.Config.BatchSize))
	Log.Info(fmt.Sprintf("Server nodes: %v", config.Config.Nodes))
}

func main() {
	Log.Info("Start Service")
	db.InitDB()
	var s *server.Server
	s, err := server.NewServer(config.Config.ServerHost, config.Config.ServerPort)
	if err != nil {
		Log.Error(fmt.Sprintf("Create Server, error: %v", err))
		s.Close()
		os.Exit(1)
	}

	err = s.Init()
	if err != nil {
		Log.Error(fmt.Sprintf("Init Server error: %v", err))
		s.Close()
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sc
		Log.Info(fmt.Sprintf("Got signal: %v", sig))
		s.Close()
	}()
	Log.Info(fmt.Sprintf("Server running!"))
	s.Serve()

	Log.Info("Close Service")
	os.Exit(0)
}
