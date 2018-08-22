package raft

import (
	"log"
	"os"
)

func debugLog(filename string, context []byte) {
	logFile, err := os.Create(filename)
	defer logFile.Close()
	if err != nil {
		log.Fatalln("open file error!")
	}
	debugLog := log.New(logFile, "[Debug]", log.Llongfile)
	debugLog.Println(string(context))
}
