// Created on 2015-07-20
// summary: config
// author: YangHaitao

package config

import (
	"fmt"
	"os"

	"github.com/go-gypsy/yaml"
)

var ConfigFile *yaml.File
var Config *ServerConfig

func Init(config_path string) error {
	cfg, err := GetConfigFile(config_path)
	if err != nil {
		return err
	}
	_, err = GetConfig(cfg)
	if err != nil {
		return err
	}
	return nil
}

func GetConfigFile(config_path string) (*yaml.File, error) {
	if ConfigFile != nil {
		return ConfigFile, nil
	}

	if _, err := os.Stat(config_path); os.IsNotExist(err) {
		fmt.Printf("Init Config (%s) error: config_path does not exist!\n", config_path)
		return ConfigFile, err
	}

	ConfigFile, err := yaml.ReadFile(config_path)
	if err != nil {
		fmt.Printf("Init Config (%s) error: %s\n", config_path, err)
		return ConfigFile, err
	}
	return ConfigFile, nil
}

func GetConfig(cfg *yaml.File) (*ServerConfig, error) {
	if Config != nil {
		return Config, nil
	}

	var err error
	Config = new(ServerConfig)

	Config.LogPath, err = cfg.Get("log_path")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['log_path'] error: %s\n", err))
		return Config, err
	}

	Config.LogLevel, err = cfg.Get("log_level")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['log_level'] error: %s\n", err))
		return Config, err
	}

	Config.ServerHost, err = cfg.Get("server_host")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['server_host'] error: %s\n", err))
		return Config, err
	}

	Config.ServerPort, err = cfg.Get("server_port")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['server_host'] error: %s\n", err))
		return Config, err
	}

	Config.TransPort, err = cfg.Get("trans_port")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['trans_host'] error: %s\n", err))
		return Config, err
	}

	serverIdTmp, err := cfg.GetInt("server_id")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['server_id'] error: %s\n", err))
		return Config, err
	}
	Config.ServerId = int(serverIdTmp)

	Config.Nodes = make([]map[string]string, 0)
	nodesNum, err := cfg.Count("nodes")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['nodes'] count error: %s\n", err))
		return Config, err
	}
	for i := 0; i < nodesNum; i++ {
		node := make(map[string]string)
		serverHost, err := cfg.Get("nodes[0].server_host")
		if err != nil {
			fmt.Printf(fmt.Sprintf("Get Config['node'] server_host error: %s\n", err))
			return Config, err
		}
		serverPort, err := cfg.Get("nodes[0].server_port")
		if err != nil {
			fmt.Printf(fmt.Sprintf("Get Config['node'] server_port error: %s\n", err))
			return Config, err
		}
		transPort, err := cfg.Get("nodes[0].trans_port")
		if err != nil {
			fmt.Printf(fmt.Sprintf("Get Config['node'] trans_port error: %s\n", err))
			return Config, err
		}
		node["server_host"] = serverHost
		node["server_port"] = serverPort
		node["trans_port"] = transPort
		Config.Nodes = append(Config.Nodes, node)
	}

	heartbeatTimeOutTmp, err := cfg.GetInt("heartbeat_time_out")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['heartbeat_time_out'] error: %s\n", err))
		return Config, err
	}
	Config.HeartbeatTimeOut = int(heartbeatTimeOutTmp)

	heartbeatTimeIntervalTmp, err := cfg.GetInt("heartbeat_time_interval")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['heartbeat_time_interval'] error: %s\n", err))
		return Config, err
	}
	Config.HeartbeatTimeInterval = int(heartbeatTimeIntervalTmp)

	threadsTmp, err := cfg.GetInt("threads")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['threads'] error: %s\n", err))
		return Config, err
	}
	Config.Threads = int(threadsTmp)

	Config.DataPath, err = cfg.Get("data_path")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['data_path'] error: %s\n", err))
		return Config, err
	}

	Config.BatchSize, err = cfg.GetInt("batch_size")
	if err != nil {
		fmt.Printf(fmt.Sprintf("Get Config['batch_size'] error: %s\n", err))
		return Config, err
	}

	return Config, nil
}
