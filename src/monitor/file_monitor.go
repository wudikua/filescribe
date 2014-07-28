package monitor

import (
	"github.com/howeyc/fsnotify"
	"log"
	"os"
	"io"
	"bufio"
)

type MFileGroup struct {
	Reading map[string] bool
	Readers map[string] *bufio.Reader 
}

type FileMonitor struct {
	Path string
	Done chan bool
	Read chan string
	watcher *fsnotify.Watcher
	files *MFileGroup
}

func NewFileMonitor(path string) (*FileMonitor) {
	monitor := &FileMonitor{
		Path:		path,
		files:		&MFileGroup{
			Reading: 	map[string] bool {},
			Readers:	map[string] *bufio.Reader {},
		},
	}
	monitor.init()
	return monitor
}

func (monitor *FileMonitor) init() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}
	monitor.watcher = watcher
	monitor.Done = make(chan bool)
	monitor.Read = make(chan string)
}

func (monitor *FileMonitor) readEOF(file string) {
	defer func(){monitor.files.Reading[file] = false}()
	
	if monitor.files.Readers[file] == nil {
		fp, err := os.Open(file)
		if err != nil {
			log.Println("opening file error %v\n", err)
			return
		}
		fp.Seek(0, 2)
		monitor.files.Readers[file] = bufio.NewReader(fp)
	}	
	for {
		line, err := monitor.files.Readers[file].ReadString('\n')
		if err == io.EOF {
			return
		} else {
			monitor.Read <- line
		}
	}
	
}

func (monitor *FileMonitor) WatchLoop() {
	err := monitor.watcher.Watch(monitor.Path)
	defer monitor.watcher.Close()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}
	for {
		select {
			case ev := <-monitor.watcher.Event:
				if ev.IsModify() {
					if !monitor.files.Reading[ev.Name] {
						monitor.files.Reading[ev.Name] = true
						go monitor.readEOF(ev.Name)
					}
				}
			case _ = <-monitor.watcher.Error:
		}
	}
	<-monitor.Done
}


