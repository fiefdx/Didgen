package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"Didgen/config"
	log "Didgen/logger_seelog"
	"Didgen/model"
	_ "github.com/mattn/go-sqlite3"
)

const (
	ConfigTableName         = "__config__"
	CreateConfigTableNTStmt = `
	CREATE TABLE IF NOT EXISTS %s (
        id Integer,
		log_level Text,
        log_path Text,
        server_host Text,
        server_port Text,
        trans_port Text,
        server_id Integer,
        nodes Text,
        heartbeat_time_out Integer,
        heartbeat_time_interval Integer,
        threads Integer,
        data_path Text,
        batch_size Integer,
		PRIMARY KEY (id)
	)`
	UpdateConfigStmt = `UPDATE %s SET %s = ? WHERE id = 1`
	InsertConfigStmt = `INSERT INTO %s (id) VALUES (1)`
	SelectConfigStmt = `SELECT log_level, log_path, server_host, server_port, trans_port, server_id, nodes, heartbeat_time_out,
	                    heartbeat_time_interval, threads, data_path, batch_size FROM %s`
	ConfigPrefix = "cfg."
)

var CONFIG *Config

type Config struct {
	DB *sql.DB
}

func InitConfig() {
	cfg := new(Config)
	cfg.InitDB()
	cfg.CreateConfigTable(false)
	cfg.UpdateConfig()
	CONFIG = cfg
}

func (c *Config) InitDB() {
	dbPath := filepath.Join(config.Config.DataPath, "configuration.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic(err)
	} else if db == nil {
		panic("db is nil")
	}
	c.DB = db
}

func (c *Config) CreateConfigTable(force bool) error {
	if force {
		sqlStmt := fmt.Sprintf(DropTableStmt, ConfigTableName)
		_, err := c.DB.Exec(sqlStmt)
		if err != nil {
			log.Error(fmt.Sprintf("Config.CreateConfigTable with force, error: %v", err))
			return err
		}
	}
	sqlStmt := fmt.Sprintf(CreateConfigTableNTStmt, ConfigTableName)
	_, err := c.DB.Exec(sqlStmt)
	if err != nil {
		log.Error(fmt.Sprintf("Config.CreateConfigTable without force, error: %v", err))
		return err
	}

	sqlStmt = fmt.Sprintf(RowCountStmt, ConfigTableName)
	rows, err := c.DB.Query(sqlStmt)
	if err != nil {
		log.Error(fmt.Sprintf("Config.CreateConfigTable, count error: %v", err))
		return err
	}
	defer rows.Close()
	var rowCount int64
	for rows.Next() {
		err = rows.Scan(&rowCount)
		if err != nil {
			log.Error(fmt.Sprintf("Config.CreateConfigTable, count error: %v", err))
			return err
		}
	}

	if rowCount == int64(0) {
		sqlStmt = fmt.Sprintf(InsertConfigStmt, ConfigTableName)
		_, err = c.DB.Exec(sqlStmt)
		if err != nil {
			log.Error(fmt.Sprintf("Config.CreateConfigTable, insert value error: %v", err))
			return err
		}
		cfg := make(map[string]interface{})
		cfg["log_level"] = config.Config.LogLevel
		cfg["log_path"] = config.Config.LogPath
		cfg["server_host"] = config.Config.ServerHost
		cfg["server_port"] = config.Config.ServerPort
		cfg["trans_port"] = config.Config.TransPort
		cfg["server_id"] = config.Config.ServerId
		nodes, _ := json.MarshalIndent(config.Config.Nodes, "", "    ")
		cfg["nodes"] = string(nodes)
		cfg["heartbeat_time_out"] = config.Config.HeartbeatTimeOut
		cfg["heartbeat_time_interval"] = config.Config.HeartbeatTimeInterval
		cfg["threads"] = config.Config.Threads
		cfg["data_path"] = config.Config.DataPath
		cfg["batch_size"] = config.Config.BatchSize
		c.Update(cfg)
	}
	return nil
}

func (c *Config) Update(keys map[string]interface{}) error {
	for key, value := range keys {
		sqlStmt := fmt.Sprintf(UpdateConfigStmt, ConfigTableName, key)
		_, err := c.DB.Exec(sqlStmt, value)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
				return nil
			}
			log.Error(fmt.Sprintf("Config.Update key: '%s', error: %v", key, err))
			return err
		}
	}

	return nil
}

func (c *Config) UpdateConfig() error {
	result := new(model.ServerConfigDB)
	sqlStmt := fmt.Sprintf(SelectConfigStmt, ConfigTableName)
	row := c.DB.QueryRow(sqlStmt)
	err := row.Scan(&result.LogLevel,
		&result.LogPath,
		&result.ServerHost,
		&result.ServerPort,
		&result.TransPort,
		&result.ServerId,
		&result.Nodes,
		&result.HeartbeatTimeOut,
		&result.HeartbeatTimeInterval,
		&result.Threads,
		&result.DataPath,
		&result.BatchSize)
	if err != nil {
		log.Error(fmt.Sprintf("Config.UpdateConfig, error: %v", err))
		return err
	}
	config.Config.LogLevel = result.LogLevel
	config.Config.LogPath = result.LogPath
	config.Config.ServerHost = result.ServerHost
	config.Config.ServerPort = result.ServerPort
	config.Config.TransPort = result.TransPort
	config.Config.ServerId = result.ServerId
	// config.Config.Nodes = result.
	config.Config.HeartbeatTimeOut = result.HeartbeatTimeOut
	config.Config.HeartbeatTimeInterval = result.HeartbeatTimeInterval
	config.Config.Threads = result.Threads
	config.Config.DataPath = result.DataPath
	config.Config.BatchSize = result.BatchSize
	return nil
}

func (c *Config) Set(key, value string) error {
	switch key {
	case "log_level", "log_path", "server_host", "server_port", "trans_port", "server_id",
		"heartbeat_time_out", "heartbeat_time_interval", "threads", "data_path", "batch_size":
		err := c.Update(map[string]interface{}{key: value})
		if err != nil {
			return err
		}
		return c.UpdateConfig()
	default:
		return fmt.Errorf("cfg.key not found!")
	}
}
