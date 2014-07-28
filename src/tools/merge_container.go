package tools

import (
	"time"
	"prism"
	"fmt"
)

type MergeEntry struct {
	Count int
	Sum int
	GroupTime int64
	UpdateTime int64
}

type MergeContainer struct {
	fields map[string] []*MergeEntry 
	context *MergeContext
}

type MergeContext struct {
	CleanTime int
	CleanSize int
}

func NewMergeContainer(context *MergeContext) (*MergeContainer) {
	container := &MergeContainer {
		context :	context,
		fields :	make(map[string] []*MergeEntry),
	}
	return container
}

func (container MergeContainer) Dump() {
	fmt.Println("=========DUMP BEGIN=========")
	for k, v := range container.fields {
		for i:=0; i<len(v); i++ {
			e := v[i]
			fmt.Println(k, e.Count, e.Sum, e.GroupTime)
		}
	}
	fmt.Println("=========DUMP END=========")
}

func (container MergeContainer) Clean(path string) {
	now := time.Now().Unix()
	entrys := container.fields[path]
	newEntrys := []*MergeEntry{}
	
	for i := 0; i < len(entrys); i++ {
		entry := *entrys[i]
		if int(now - entry.UpdateTime) >= container.context.CleanTime {
		 	prism.ImportOneRow(
		 		"ALL", 
		 		time.Unix(entry.GroupTime, 0).Format("2006-01-02 15:04:05"),
 				path,  
		 		entry.Sum,
		 		entry.Count,
		 		entry.Sum / entry.Count,
		 		time.Unix(entry.GroupTime, 0).Format("1021504"),
		 	)
		} else {
			newEntrys = append(newEntrys, entrys[i])
		}
	}
	container.fields[path] = newEntrys
}

func (container MergeContainer) Add(path string, groupTime int64, cost int) {
	if container.fields[path] == nil {
		entrys := []*MergeEntry{}
		entrys = append(entrys, &MergeEntry{
			Count :		1,
			Sum :		cost,
			GroupTime :	groupTime,
		})
		container.fields[path] = entrys
	} else {
		entrys := container.fields[path]
		flag := false
		for i := len(entrys) - 1; i >= 0; i-- {
			if (*entrys[i]).GroupTime == groupTime {
				flag = true
				(*entrys[i]).Count = (*entrys[i]).Count + 1
				(*entrys[i]).Sum = (*entrys[i]).Sum + cost
			}
		}
		if !flag {
			entrys = append(entrys, &MergeEntry{
				Count :			1,
				Sum :			cost,
				GroupTime :		groupTime,
				UpdateTime :	time.Now().Unix(),
			})
			container.fields[path] = entrys
		}
		if len(entrys) >= container.context.CleanSize {
			container.Clean(path)
		}
	}
}

