package server

import (
	"strconv"
	"strings"

	"Didgen/config"
	"Didgen/db"
)

func (s *Server) handleGet(r *Request) Reply {
	var idgen *db.IdGenerator
	var ok bool
	var id int64
	var err error
	var idStr string

	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	key := string(r.Arguments[0])
	if len(key) == 0 {
		return ErrNoKey
	}

	if strings.HasPrefix(key, db.ConfigPrefix) {
		k := key[len(db.ConfigPrefix):]
		idStr, err = config.Config.Get(k)
		if err != nil {
			return &BulkReply{
				value: nil,
			}
		}
		return &StatusReply{
			code: idStr,
		}
	} else {
		s.Lock()
		idgen, ok = s.keyGeneratorMap[key]

		if ok == false {
			s.Unlock()
			return &BulkReply{
				value: nil,
			}
		}

		s.Unlock()
		id, err = idgen.Next()
		if err != nil {
			return &ErrorReply{
				message: err.Error(),
			}
		}
		idStr = strconv.FormatInt(id, 10)
	}

	return &BulkReply{
		value: []byte(idStr),
	}
}

//redis command(set abc 12)
func (s *Server) handleSet(r *Request) Reply {
	var idgen *db.IdGenerator
	var ok bool
	var err error

	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	key := string(r.Arguments[0])
	if len(key) == 0 {
		return ErrNoKey
	}

	if strings.HasPrefix(key, db.ConfigPrefix) {
		k := key[len(db.ConfigPrefix):]
		value, errReply := r.GetString(1)
		if errReply != nil {
			return errReply
		}
		err = db.CONFIG.Set(k, value)
		if err != nil {
			return &ErrorReply{
				message: err.Error(),
			}
		}
	} else {
		value, errReply := r.GetInt(1)
		if errReply != nil {
			return errReply
		}

		s.Lock()
		idgen, ok = s.keyGeneratorMap[key]
		if ok == false {
			idgen, err = db.NewIdGenerator(key)
			if err != nil {
				s.Unlock()
				return &ErrorReply{
					message: err.Error(),
				}
			}
			s.keyGeneratorMap[key] = idgen
		}

		s.Unlock()
		err = s.SetKey(key)
		if err != nil {
			return &ErrorReply{
				message: err.Error(),
			}
		}

		err = idgen.Reset(value, false)
		if err != nil {
			return &ErrorReply{
				message: err.Error(),
			}
		}
	}

	return &StatusReply{
		code: "OK",
	}
}

func (s *Server) handleExists(r *Request) Reply {
	var ok bool
	var id int64

	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	key := string(r.Arguments[0])
	if len(key) == 0 {
		return ErrNoKey
	}
	s.Lock()
	_, ok = s.keyGeneratorMap[key]
	s.Unlock()
	if ok {
		id = 1
	}

	return &IntReply{
		number: id,
	}
}

func (s *Server) handleDel(r *Request) Reply {
	var idgen *db.IdGenerator
	var ok bool
	var id int64 = 0

	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	key := string(r.Arguments[0])
	if len(key) == 0 {
		return ErrNoKey
	}
	s.Lock()
	idgen, ok = s.keyGeneratorMap[key]
	if ok {
		delete(s.keyGeneratorMap, key)
	}
	s.Unlock()
	if ok {
		err := idgen.Delete()
		if err != nil {
			return &ErrorReply{
				message: err.Error(),
			}
		}
		err = s.DelKey(key)
		if err != nil {
			return &ErrorReply{
				message: err.Error(),
			}
		}
		id = 1
	}

	return &IntReply{
		number: id,
	}
}

func (s *Server) handleSelect(r *Request) Reply {
	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	num := string(r.Arguments[0])
	if len(num) == 0 {
		return ErrNotEnoughArgs
	}

	return &StatusReply{
		code: "OK",
	}
}
