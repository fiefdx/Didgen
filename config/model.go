package config

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
