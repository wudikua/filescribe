package main

import (
	"flag"
	"logger"
	"monitor"
)

var filepath = flag.String("f", "", "the file or floder to be monitoied")
var category = flag.String("c", "gotest", "log category") 

func main() {
	flag.Parse()
	pool := logger.NewLoggerPool(*category)
	defer pool.Destory()
	monitor := monitor.NewFileMonitor(*filepath)
	go monitor.WatchLoop()
	for {
		select {
			case line := <-monitor.Read:
			pool.Log(line)
		}
	}
}

