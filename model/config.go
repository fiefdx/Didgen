package model

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type ServerConfig struct {
	LogLevel              string
	LogPath               string
	ServerHost            string
	ServerPort            string
	TransPort             string
	ServerId              int
	Nodes                 []map[string]string
	HeartbeatTimeOut      int
	HeartbeatTimeInterval int
	Threads               int
	DataPath              string
	BatchSize             int64
}

func (c *ServerConfig) Get(key string) (string, error) {
	switch key {
	case "log_level":
		return c.LogLevel, nil
	case "log_path":
		return c.LogPath, nil
	case "server_host":
		return c.ServerHost, nil
	case "server_port":
		return c.ServerPort, nil
	case "trans_port":
		return c.TransPort, nil
	case "server_id":
		return strconv.FormatInt(int64(c.ServerId), 10), nil
	case "nodes":
		nodes, _ := json.MarshalIndent(c.Nodes, "", "    ")
		return string(nodes), nil
	case "heartbeat_time_out":
		return strconv.FormatInt(int64(c.HeartbeatTimeOut), 10), nil
	case "heartbeat_time_interval":
		return strconv.FormatInt(int64(c.HeartbeatTimeInterval), 10), nil
	case "threads":
		return strconv.FormatInt(int64(c.Threads), 10), nil
	case "data_path":
		return c.DataPath, nil
	case "batch_size":
		return strconv.FormatInt(c.BatchSize, 10), nil
	default:
		return "", fmt.Errorf("cfg.key not found!")
	}
}

type ServerConfigDB struct {
	LogLevel              string
	LogPath               string
	ServerHost            string
	ServerPort            string
	TransPort             string
	ServerId              int
	Nodes                 string
	HeartbeatTimeOut      int
	HeartbeatTimeInterval int
	Threads               int
	DataPath              string
	BatchSize             int64
}
