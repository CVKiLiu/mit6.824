package raft

import (
	"log"
	"os"
	"strconv"
	"time"
)

type logPrefix string

const (
	LOG_ENTRIES_INFO string = "LOG_ENTRIES_INFO"
	RAFT_INFO               = "RAFT_INFO"
)

const (
	INFO     logPrefix = "INFO"
	WARNNING           = "WARNING"
	ERROR              = "ERROR"
)

type rfLogger struct {
	start time.Time
}

func newLogger(start time.Time) rfLogger {
	rl := rfLogger{
		start: start,
	}
	return rl
}

func (rl *rfLogger) raftInfo(raft Raft, prefix logPrefix, info interface{}) {

	filename := LOG_ENTRIES_INFO + "_" + rl.start.Format(time.ANSIC) + "_" + strconv.Itoa(raft.me) + ".log"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal("Open log fail")
	}
	debugLog := log.New(file, string(prefix), log.Ltime)
	debugLog.Println("Extra", info)
	debugLog.Print("=======================\n")
	debugLog.Println("Raft Index:     ", raft.me)
	debugLog.Println("CurrentTerm:    ", raft.currentTerm)
	debugLog.Println("State:          ", raft.state)
	//debugLog.Println("voteFor:        ", raft.voteFor)
	//debugLog.Println("voteCount:      ", raft.voteCount)
	//debugLog.Println("=======================")
	debugLog.Print("\n\n")
}

func (rl *rfLogger) logInfo(raft Raft, prefix logPrefix, info interface{}) {
	filename := LOG_ENTRIES_INFO + "_" + rl.start.Format(time.ANSIC) + strconv.Itoa(raft.me) + ".log"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal("Open log fail")
	}
	debugLog := log.New(file, string(prefix), log.Ltime)
	debugLog.Println(info)
	debugLog.Println("------------------------")
	debugLog.Println("Raft Index:     ", raft.me)
	debugLog.Println("Raft log")
	//debugLog.Println("length: ", strconv.Itoa(len(raft.logEntries)))
	//for _, log := range raft.logEntries {
	//	debugLog.Println("Index: ", log.Index, "Term: ", log.Term)
	//}
	debugLog.Println("------------------------")
}
