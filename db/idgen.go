package db

import (
	"fmt"
	"sync"

	"Didgen/config"
)

type IdGenerator struct {
	key       string // id generator key name
	cur       int64  // current id
	batchMax  int64  // max id before get from db
	batchSize int64  // batch size

	lock sync.Mutex
}

func NewIdGenerator(key string) (*IdGenerator, error) {
	idgen := new(IdGenerator)
	if len(key) == 0 {
		return nil, fmt.Errorf("key is empty")
	}
	idgen.key = key
	batchSizeTmp, err := config.Config.GetInt("batch_size")
	if err != nil {
		return nil, fmt.Errorf("fetch batch_size error")
	}
	idgen.batchSize = batchSizeTmp
	idgen.cur = 0
	idgen.batchMax = idgen.cur
	return idgen, nil
}

func (g *IdGenerator) getIdFromDB() (int64, error) {
	id, err := GetKey(g.key)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (g *IdGenerator) Current() (int64, error) {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.cur, nil
}

func (g *IdGenerator) Next() (int64, error) {
	var id int64
	var err error
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.batchMax < g.cur+1 {
		id, err = GetKey(g.key)
		if err != nil {
			return 0, err
		}
		err = IncrKey(g.key, g.batchSize)
		if err != nil {
			return 0, err
		}
		g.batchMax = id + g.batchSize
		g.cur = id
	}
	g.cur++
	return g.cur, nil
}

func (g *IdGenerator) Reset(value int64, force bool) error {
	var err error
	g.lock.Lock()
	defer g.lock.Unlock()
	if force {
		err = DeleteKeyTable(g.key)
		if err != nil {
			return err
		}
	}

	err = CreateKeyTable(g.key)
	if err != nil {
		return err
	}

	err = ResetKeyTable(g.key, value)
	if err != nil {
		return err
	}
	g.cur = value
	g.batchMax = g.cur
	return nil
}

func (g *IdGenerator) Delete() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	err := DeleteKeyTable(g.key)
	if err != nil {
		return err
	}
	return nil
}
