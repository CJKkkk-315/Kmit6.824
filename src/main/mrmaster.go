package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/src/mr"
	"strings"
)
import "time"
import "os"

func main() {
	//if len(os.Args) < 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
	//	os.Exit(1)
	//}

	//m := mr.MakeMaster(os.Args[1:], 10)
	txtFiles := make([]string, 0)
	files, _ := os.ReadDir(".")
	for _, file := range files {
		name := file.Name()
		// 检查文件名是否以"pg"开头
		if strings.HasPrefix(name, "pg") {
			txtFiles = append(txtFiles, name)
		}
	}
	m := mr.MakeMaster(txtFiles, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
