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
	"github.com/go-gypsy/yaml"
	_ "net/http/pprof"
)

var Log seelog.LoggerInterface
var Config *yaml.File
var ServerHost string
var ServerPort string
var Threads int

func init() {
	configPath := "./configuration.yml"

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Init Config (%s) error: (%s) does not exist!\n", configPath)
		os.Exit(1)
	} else {
		Config = config.GetConfig(configPath)
	}

	logPath, err := Config.Get("log_path")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['log_path'] error: %s\n", err))
		os.Exit(1)
	}

	logLevel, err := Config.Get("log_level")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['log_level'] error: %s\n", err))
		os.Exit(1)
	}

	Logger, err := logger.NewLogger("main", logPath, "didgen.log", logLevel, "size", "20971520", "5", true)
	if err != nil {
		fmt.Printf(fmt.Sprintf("Init logger error: %s\n", err))
		os.Exit(1)
	}
	Log = *Logger

	db.InitLog()
	server.InitLog()

	tmp_server_host, err := Config.Get("server_host")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['server_host'] error: %s\n", err))
		os.Exit(1)
	}
	ServerHost = tmp_server_host

	tmp_server_port, err := Config.Get("server_port")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['server_host'] error: %s\n", err))
		os.Exit(1)
	}
	ServerPort = string(tmp_server_port)

	dataPath, err := Config.Get("data_path")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['data_path'] error: %s\n", err))
		os.Exit(1)
	}

	threads_tmp, err := Config.GetInt("threads")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['threads'] error: %s\n", err))
		os.Exit(1)
	}
	Threads = int(threads_tmp)

	threads := runtime.GOMAXPROCS(Threads)
	Log.Info(fmt.Sprintf("Server with threads: %d", threads))
	Log.Info(fmt.Sprintf("Server config path: %s", configPath))
	Log.Info(fmt.Sprintf("Server log path: %s", logPath))
	Log.Info(fmt.Sprintf("Server log level: %s", logLevel))
	Log.Info(fmt.Sprintf("Server host: %s", ServerHost))
	Log.Info(fmt.Sprintf("Server port: %s", ServerPort))
	Log.Info(fmt.Sprintf("Server data path: %s", dataPath))
}

func main() {
	Log.Info("Start Service")
	db.InitDB()
	var s *server.Server
	s, err := server.NewServer(ServerHost, ServerPort)
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
