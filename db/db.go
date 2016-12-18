package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"Didgen/config"
	logger "Didgen/logger_seelog"
	"github.com/cihub/seelog"
	_ "github.com/mattn/go-sqlite3"
)

const (
	KeysRecordTableName     = "__idgen__"
	CreateRecordTableNTStmt = `
	CREATE TABLE IF NOT EXISTS %s (
		k VARCHAR(255) NOT NULL,
		PRIMARY KEY (k)
	)`
	InsertKeyStmt  = "INSERT INTO %s (k) VALUES ('%s')"
	SelectKeyStmt  = "SELECT k FROM %s WHERE k = '%s'"
	SelectKeysStmt = "SELECT k FROM %s"
	DeleteKeyStmt  = "DELETE FROM %s WHERE k = '%s'"

	KeyPrefixFmt = "idgen_%s"
	//create key table
	CreateKeyTableStmt = `
	CREATE TABLE %s (
		id bigint,
		PRIMARY KEY  (id)
	)`

	//create key table if not exist
	CreateKeyTableNTStmt = `
	CREATE TABLE IF NOT EXISTS %s (
		id bigint,
		PRIMARY KEY  (id)
	)`

	DropTableStmt    = `DROP TABLE IF EXISTS %s`
	InsertIdStmt     = "INSERT INTO %s (id) VALUES (%d)"
	SelectIdStmt     = "SELECT id FROM %s"
	UpdateIdIncrStmt = "UPDATE %s SET id = id + %d"
	UpdateIdStmt     = "UPDATE %s SET id = %d"
	RowCountStmt     = "SELECT count(*) FROM %s"
	GetKeysStmt      = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'"

	BatchCount = 1
)

var Log seelog.LoggerInterface
var DB *sql.DB

func InitLog() {
	Logger, err := logger.GetLogger("main")
	if err != nil {
		panic(fmt.Errorf("GetLogger error: %s\n", err))
	}
	Log = *Logger
}

func InitDB() {
	dataPath, _ := config.Config.Get("data_path")
	dbPath := filepath.Join(dataPath, "data.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic(err)
	} else if db == nil {
		panic("db is nil")
	}
	DB = db
}

func CreateKeysRecordTable(force bool) error {
	if force {
		sqlStmt := fmt.Sprintf(DropTableStmt, KeysRecordTableName)
		_, err := DB.Exec(sqlStmt)
		if err != nil {
			Log.Info(fmt.Sprintf("CreateKeysRecordTable with force, error: %v", err))
			return err
		}
	}
	sqlStmt := fmt.Sprintf(CreateRecordTableNTStmt, KeysRecordTableName)
	_, err := DB.Exec(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("CreateKeysRecordTable without force, error: %v", err))
		return err
	}
	return nil
}

func AddKeyToRecordTable(key string) error {
	sqlStmt := fmt.Sprintf(InsertKeyStmt, KeysRecordTableName, key)
	_, err := DB.Exec(sqlStmt)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
			return nil
		}
		Log.Info(fmt.Sprintf("AddKeyToRecordTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func GetKeyFromRecordTable(key string) (string, error) {
	var result string
	sqlStmt := fmt.Sprintf(SelectKeyStmt, KeysRecordTableName, key)
	row := DB.QueryRow(sqlStmt)
	err := row.Scan(&result)
	if err != nil {
		Log.Info(fmt.Sprintf("GetKeyFromRecordTable('%s'), error: %v", key, err))
		return "", err
	}
	return result, nil
}

func GetKeysFromRecordTable() ([]string, error) {
	result := make([]string, 0)
	sqlStmt := fmt.Sprintf(SelectKeysStmt, KeysRecordTableName)
	rows, err := DB.Query(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("GetKeysFromRecordTable, error: %v", err))
		return result, err
	}
	defer rows.Close()
	var key string
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			Log.Error(fmt.Sprintf("GetKeysFromRecordTable, row error: %v", err))
			return result, err
		}
		if key != "" {
			result = append(result, key)
		}
	}
	return result, nil
}

func DeleteKeyFromRecordTable(key string) error {
	sqlStmt := fmt.Sprintf(DeleteKeyStmt, KeysRecordTableName, key)
	_, err := DB.Exec(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("DeleteKeyFromRecordTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func FmtKey(key string) string {
	return fmt.Sprintf(KeyPrefixFmt, key)
}

func CreateKeyTable(key string) error {
	idKey := FmtKey(key)
	sqlStmt := fmt.Sprintf(CreateKeyTableNTStmt, idKey)
	_, err := DB.Exec(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("CreateKeyTable('%s'), error: %v", key, err))
		return err
	}
	sqlStmt = fmt.Sprintf(RowCountStmt, idKey)
	rows, err := DB.Query(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("CreateKeyTable('%s'), count error: %v", key, err))
		return err
	}
	defer rows.Close()
	var rowCount int64
	for rows.Next() {
		err = rows.Scan(&rowCount)
		if err != nil {
			Log.Error(fmt.Sprintf("CreateKeyTable('%s'), count error: %v", key, err))
			return err
		}
	}

	if rowCount == int64(0) {
		sqlStmt = fmt.Sprintf(InsertIdStmt, idKey, 0)
		_, err = DB.Exec(sqlStmt)
		if err != nil {
			Log.Info(fmt.Sprintf("CreateKeyTable('%s'), insert value error: %v", key, err))
			return err
		}
	}

	return nil
}

func ResetKeyTable(key string, value int64) error {
	idKey := FmtKey(key)
	sqlStmt := fmt.Sprintf(UpdateIdStmt, idKey, value)
	_, err := DB.Exec(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("ResetKeyTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func DeleteKeyTable(key string) error {
	idKey := FmtKey(key)
	sqlStmt := fmt.Sprintf(DropTableStmt, idKey)
	_, err := DB.Exec(sqlStmt)
	if err != nil {
		Log.Info(fmt.Sprintf("DeleteKeyTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func IncrKey(key string, value int64) error {
	idKey := FmtKey(key)
	sqlStmt := fmt.Sprintf(UpdateIdIncrStmt, idKey, value)
	tx, err := DB.Begin()
	if err != nil {
		Log.Error(fmt.Sprintf("IncrKey('%s'), value: %d, error: %v", key, value, err))
	}
	_, err = tx.Exec(sqlStmt)
	if err != nil {
		tx.Rollback()
		Log.Error(fmt.Sprintf("IncrKey('%s'), value: %d, error: %v", key, value, err))
		return err
	}
	tx.Commit()
	return nil
}

func GetKey(key string) (int64, error) {
	var id int64
	idKey := FmtKey(key)
	sqlStmt := fmt.Sprintf(SelectIdStmt, idKey)
	row := DB.QueryRow(sqlStmt)
	err := row.Scan(&id)
	if err != nil {
		Log.Error(fmt.Sprintf("GetKey('%s'), error: %v", key, err))
		return 0, err
	}
	return id, nil
}
