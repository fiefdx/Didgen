package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"Didgen/config"
	log "Didgen/logger_seelog"
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

	KeyPrefixFmt       = "idgen_%s"
	CreateKeyTableStmt = `
	CREATE TABLE %s (
		id bigint,
		PRIMARY KEY  (id)
	)`
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

var DATA *Data

type Data struct {
	DB *sql.DB
}

func InitData() {
	data := new(Data)
	data.InitDB()
	DATA = data
}

func (d *Data) InitDB() {
	dbPath := filepath.Join(config.Config.DataPath, "data.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic(err)
	} else if db == nil {
		panic("db is nil")
	}
	d.DB = db
}

func (d *Data) CreateKeysRecordTable(force bool) error {
	if force {
		sqlStmt := fmt.Sprintf(DropTableStmt, KeysRecordTableName)
		_, err := d.DB.Exec(sqlStmt)
		if err != nil {
			log.Info(fmt.Sprintf("Data.CreateKeysRecordTable with force, error: %v", err))
			return err
		}
	}
	sqlStmt := fmt.Sprintf(CreateRecordTableNTStmt, KeysRecordTableName)
	_, err := d.DB.Exec(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.CreateKeysRecordTable without force, error: %v", err))
		return err
	}
	return nil
}

func (d *Data) AddKeyToRecordTable(key string) error {
	sqlStmt := fmt.Sprintf(InsertKeyStmt, KeysRecordTableName, key)
	_, err := d.DB.Exec(sqlStmt)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
			return nil
		}
		log.Info(fmt.Sprintf("Data.AddKeyToRecordTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func (d *Data) GetKeyFromRecordTable(key string) (string, error) {
	var result string
	sqlStmt := fmt.Sprintf(SelectKeyStmt, KeysRecordTableName, key)
	row := d.DB.QueryRow(sqlStmt)
	err := row.Scan(&result)
	if err != nil {
		log.Info(fmt.Sprintf("Data.GetKeyFromRecordTable('%s'), error: %v", key, err))
		return "", err
	}
	return result, nil
}

func (d *Data) GetKeysFromRecordTable() ([]string, error) {
	result := make([]string, 0)
	sqlStmt := fmt.Sprintf(SelectKeysStmt, KeysRecordTableName)
	rows, err := d.DB.Query(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.GetKeysFromRecordTable, error: %v", err))
		return result, err
	}
	defer rows.Close()
	var key string
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			log.Error(fmt.Sprintf("Data.GetKeysFromRecordTable, row error: %v", err))
			return result, err
		}
		if key != "" {
			result = append(result, key)
		}
	}
	return result, nil
}

func (d *Data) DeleteKeyFromRecordTable(key string) error {
	sqlStmt := fmt.Sprintf(DeleteKeyStmt, KeysRecordTableName, key)
	_, err := d.DB.Exec(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.DeleteKeyFromRecordTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func (d *Data) FmtKey(key string) string {
	return fmt.Sprintf(KeyPrefixFmt, key)
}

func (d *Data) CreateKeyTable(key string) error {
	idKey := d.FmtKey(key)
	sqlStmt := fmt.Sprintf(CreateKeyTableNTStmt, idKey)
	_, err := d.DB.Exec(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.CreateKeyTable('%s'), error: %v", key, err))
		return err
	}
	sqlStmt = fmt.Sprintf(RowCountStmt, idKey)
	rows, err := d.DB.Query(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.CreateKeyTable('%s'), count error: %v", key, err))
		return err
	}
	defer rows.Close()
	var rowCount int64
	for rows.Next() {
		err = rows.Scan(&rowCount)
		if err != nil {
			log.Error(fmt.Sprintf("Data.CreateKeyTable('%s'), count error: %v", key, err))
			return err
		}
	}

	if rowCount == int64(0) {
		sqlStmt = fmt.Sprintf(InsertIdStmt, idKey, 0)
		_, err = d.DB.Exec(sqlStmt)
		if err != nil {
			log.Info(fmt.Sprintf("Data.CreateKeyTable('%s'), insert value error: %v", key, err))
			return err
		}
	}

	return nil
}

func (d *Data) ResetKeyTable(key string, value int64) error {
	idKey := d.FmtKey(key)
	sqlStmt := fmt.Sprintf(UpdateIdStmt, idKey, value)
	_, err := d.DB.Exec(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.ResetKeyTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func (d *Data) DeleteKeyTable(key string) error {
	idKey := d.FmtKey(key)
	sqlStmt := fmt.Sprintf(DropTableStmt, idKey)
	_, err := d.DB.Exec(sqlStmt)
	if err != nil {
		log.Info(fmt.Sprintf("Data.DeleteKeyTable('%s'), error: %v", key, err))
		return err
	}
	return nil
}

func (d *Data) IncrKey(key string, value int64) error {
	idKey := d.FmtKey(key)
	sqlStmt := fmt.Sprintf(UpdateIdIncrStmt, idKey, value)
	tx, err := d.DB.Begin()
	if err != nil {
		log.Error(fmt.Sprintf("Data.IncrKey('%s'), value: %d, error: %v", key, value, err))
	}
	_, err = tx.Exec(sqlStmt)
	if err != nil {
		tx.Rollback()
		log.Error(fmt.Sprintf("Data.IncrKey('%s'), value: %d, error: %v", key, value, err))
		return err
	}
	tx.Commit()
	return nil
}

func (d *Data) GetKey(key string) (int64, error) {
	var id int64
	idKey := d.FmtKey(key)
	sqlStmt := fmt.Sprintf(SelectIdStmt, idKey)
	row := d.DB.QueryRow(sqlStmt)
	err := row.Scan(&id)
	if err != nil {
		log.Error(fmt.Sprintf("Data.GetKey('%s'), error: %v", key, err))
		return 0, err
	}
	return id, nil
}
