package main

import (
	"fmt"
	"github.com/artyom/scribe"
	"github.com/artyom/thrift"
	"github.com/artyom/fb303"
	"os"
	"regexp"
	"strconv"
	"runtime"
	"tools"
	"time"
)

const (
	NetworkAddr = "prism002.m6:19090"
)

var container = tools.NewMergeContainer(&tools.MergeContext{
	CleanTime : 120, 
	CleanSize : 2,
})


type ScribeProcessorImpl struct {
	*fb303.FacebookServiceClient
}

func (this *ScribeProcessorImpl) Log(messages []*scribe.LogEntry) (r scribe.ResultCode, err error) {
	reg, _ := regexp.Compile(`([^ ]*) [^ ]* [^ ]* .*?\[(.*?\d{4}:\d{2}:\d{2}).*?\] .*?(POST|GET)\s+([^? ]+).*?" (\d+) .*"(.*)" "([^"]*)" ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*)`)
	for _, entry := range messages {
		if reg.MatchString(entry.Message) {
			matches := reg.FindSubmatch([]byte(entry.Message))
			cost, _ := strconv.ParseFloat(string(matches[9]), 64)
			logTime, _ := time.Parse("02/Jan/2006:15:04", string(matches[2]));
			host := string(matches[10])
			if len(host) < 5 {
				continue
			}
			container.Add(
				host + "@access", 
				logTime.Unix(),
				int(cost*1000),
			)
		}
	}
	return scribe.ResultCode_OK, nil
}


func main() {
	runtime.GOMAXPROCS(24)
	fmt.Println("helo")
	
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	serverTransport, err := thrift.NewTServerSocket(NetworkAddr)
	if err != nil {
		fmt.Println("Error!", err)
		os.Exit(1)
	}

	handler := &ScribeProcessorImpl{}
	processor := scribe.NewScribeProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
	fmt.Println("thrift server in", NetworkAddr)
	
	go func() {
		for {
			time.Sleep(5 * time.Second)
			container.Dump()
		}
	}()
	
	server.Serve()
}
