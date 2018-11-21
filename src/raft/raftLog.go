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
	raft  *Raft
	start time.Time
}

func newlogger(rf *Raft, start time.Time) rfLogger {
	rl := rfLogger{
		raft:  rf,
		start: start,
	}
	return rl
}

func (rl *rfLogger) raftInfo(prefix logPrefix, info interface{}) {

	filename := LOG_ENTRIES_INFO + "_" + rl.start.Format(time.ANSIC) + "_" + strconv.Itoa(rl.raft.me) + ".log"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal("Open log fail")
	}
	debugLog := log.New(file, string(prefix), log.Ltime)
	debugLog.Println("Extra", info)
	debugLog.Print("=======================\n")
	debugLog.Println("Raft Index:     ", rl.raft.me)
	debugLog.Println("CurrentTerm:    ", rl.raft.currentTerm)
	debugLog.Println("State:          ", rl.raft.state)
	debugLog.Println("voteFor:        ", rl.raft.voteFor)
	debugLog.Println("voteCount:      ", rl.raft.voteCount)
	debugLog.Println("=======================")
	debugLog.Print("\n\n")
}

func (rl *rfLogger) logInfo(prefix logPrefix, info interface{}) {
	filename := LOG_ENTRIES_INFO + "_" + rl.start.Format(time.ANSIC) + strconv.Itoa(rl.raft.me) + ".log"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer file.Close()
	if err != nil {
		log.Fatal("Open log fail")
	}
	debugLog := log.New(file, string(prefix), log.Ltime)
	debugLog.Println(info)
	debugLog.Println("------------------------")
	debugLog.Println("Raft Index:     ", rl.raft.me)
	debugLog.Println("Raft log")
	debugLog.Println("length: ", strconv.Itoa(len(rl.raft.logEntries)))
	for _, log := range rl.raft.logEntries {
		debugLog.Println("Index: ", log.Index, "Term: ", log.Term)
	}
	debugLog.Println("------------------------")
}
