package logger

import (
	
)

const POOL_SIZE = 4 

type LoggerPool struct {
	logger [POOL_SIZE]*Logger
	randIndex int
}


func NewLoggerPool(category string) (*LoggerPool) {
	pool := &LoggerPool {
		randIndex:	0,
	}
	for i:=0; i<POOL_SIZE; i++ {
		pool.logger[i] = NewLogger(category)
		go pool.logger[i].WaitLogEvent()
	}
	return pool
}


func (pool *LoggerPool) Log(line string) {
	logger := pool.logger[pool.randIndex]
	logger.Read <- line
	pool.randIndex = (pool.randIndex + 1) % POOL_SIZE
}

func (pool *LoggerPool) Destory() {
	for i:=0; i<POOL_SIZE; i++ {
		pool.logger[i].close()
	}
}
