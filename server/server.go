package server

import (
	"fmt"
	"net"
	"runtime"
	"sync"

	"Didgen/db"
	logger "Didgen/logger_seelog"
	"github.com/cihub/seelog"
)

var Log seelog.LoggerInterface

func InitLog() {
	Logger, err := logger.GetLogger("main")
	if err != nil {
		panic(fmt.Errorf("GetLogger error: %s\n", err))
	}
	Log = *Logger
}

type Server struct {
	listener        net.Listener
	keyGeneratorMap map[string]*db.IdGenerator
	sync.RWMutex
	running bool
}

func NewServer(host, port string) (*Server, error) {
	var err error
	s := new(Server)
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return nil, err
	}
	s.keyGeneratorMap = make(map[string]*db.IdGenerator)
	Log.Info(fmt.Sprintf("NewServer(%s:%s)", host, port))
	return s, nil
}

func (s *Server) Init() error {
	var err error
	err = db.CreateKeysRecordTable(false)
	if err != nil {
		return err
	}
	keys, err := db.GetKeysFromRecordTable()
	for _, key := range keys {
		idgen, ok := s.keyGeneratorMap[key]
		if !ok {
			err = db.CreateKeyTable(key)
			if err != nil {
				return err
			}
			idgen, err = db.NewIdGenerator(key)
			if err != nil {
				return err
			}
			s.keyGeneratorMap[key] = idgen
		}
	}
	return nil
}

func (s *Server) Serve() error {
	s.running = true
	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			Log.Error(fmt.Sprintf("Server Run error: %v", err))
			continue
		}
		go s.onConn(conn)
	}
	return nil
}

func (s *Server) onConn(conn net.Conn) error {
	defer func() {
		clientAddr := conn.RemoteAddr().String()
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			Log.Error(fmt.Sprintf("Server onConn remoteAddr[%v], stack[%v], error: %v", clientAddr, string(buf), err))
			reply := &ErrorReply{
				message: err.Error(),
			}
			reply.WriteTo(conn)
		}
		conn.Close()
	}()

	for {
		request, err := NewRequest(conn)
		if err != nil {
			return err
		}

		reply := s.ServeRequest(request)
		if _, err := reply.WriteTo(conn); err != nil {
			Log.Error(fmt.Sprint("server onConn reply write error: %v", err))
			return err
		}
	}
	return nil
}

func (s *Server) ServeRequest(request *Request) Reply {
	switch request.Command {
	case "GET":
		return s.handleGet(request)
	case "SET":
		return s.handleSet(request)
	case "EXISTS":
		return s.handleExists(request)
	case "DEL":
		return s.handleDel(request)
	case "SELECT":
		return s.handleSelect(request)
	default:
		return ErrMethodNotSupported
	}

	return nil
}

func (s *Server) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
	Log.Info("Server closed")
}

func (s *Server) IsKeyExist(key string) (bool, error) {
	_, err := db.GetKeyFromRecordTable(key)
	if err != nil {
		return false, err
	}
	_, err = db.GetKey(key)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) GetKey(key string) (string, error) {
	return db.GetKeyFromRecordTable(key)
}

func (s *Server) SetKey(key string) error {
	return db.AddKeyToRecordTable(key)
}

func (s *Server) DelKey(key string) error {
	return db.DeleteKeyFromRecordTable(key)
}
