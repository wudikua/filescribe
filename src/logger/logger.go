package logger

import (
	"github.com/artyom/scribe"
	"github.com/artyom/thrift"
	"net"
	"log"
	"os"
	"time"
)

const BUFFER_SIZE = 8

type Logger struct {
	category string
	client *scribe.ScribeClient
	transport *thrift.TSocket
	size int
	buffer []*scribe.LogEntry
	reconnect int64
	Read chan string
}

func NewLogger(category string) (*Logger) {
	logger := &Logger {
		category:	category,
		size:		0,
		buffer: 	make([]*scribe.LogEntry, BUFFER_SIZE, BUFFER_SIZE),
		reconnect:	0,
		Read:		make(chan string, 10),
	}
	
	logger.pconnect()
	return logger
}

func (logger *Logger) WaitLogEvent() {
	for {
		select {
			case line := <- logger.Read:
			logger.AddLog(line)
		}
	}	
}

func (logger *Logger) AddLog(line string) {
	if (logger.size == BUFFER_SIZE) {
		logger.FlushLog()
	}
	entry := scribe.NewLogEntry()
	entry.Category = logger.category
	entry.Message = line
	logger.buffer[logger.size] = entry
	logger.size++
}

func (logger *Logger) FlushLog() {
	if _, err := logger.client.Log(logger.buffer); err != nil {
		log.Println("发送 LOG失败 准备重连")
		now := time.Now().Unix()
		if now > logger.reconnect {
			logger.pconnect()
		}
	}
	logger.size = 0;
}

func (logger *Logger) pconnect() {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	logger.transport, _ = thrift.NewTSocket(net.JoinHostPort("localhost", "19090"))
	useTransport := transportFactory.GetTransport(logger.transport)
	logger.client = scribe.NewScribeClientFactory(useTransport, protocolFactory)
	if err := logger.transport.Open(); err != nil {
		log.Println("Error opening socket to localhost:19090")
		os.Exit(1)
	}
}

func (logger *Logger) close() {
	if logger.transport != nil {
		logger.transport.Close()
	}
}
