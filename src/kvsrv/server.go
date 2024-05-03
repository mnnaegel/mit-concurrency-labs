package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu                 sync.Mutex
	kvps               map[string]string
	processedCallCount map[int64]int    // clientid -> successfulCallsProcessed
	lastAppendResult   map[int64]string // no hacks to keep track of last res since can't directly derive old value (only populated if last call was append)
}

func (kv *KVServer) cleanup(clientId int64) {
	kv.processedCallCount[clientId]++
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.kvps[args.Key]; !ok {
		reply.Value = ""
		return
	}
	reply.Value = kv.kvps[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// we are synchronized
	if kv.processedCallCount[args.ClientId] == args.ClientSuccessfulCallCount {
		defer kv.cleanup(args.ClientId)
	} else {
		reply.Value = args.Value
		return
	}

	kv.kvps[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.processedCallCount[args.ClientId] != args.ClientSuccessfulCallCount {
		reply.Value = kv.lastAppendResult[args.ClientId]
		return
	}

	defer kv.cleanup(args.ClientId)

	if _, ok := kv.kvps[args.Key]; !ok {
		reply.Value = ""
		kv.kvps[args.Key] = args.Value
	} else {
		reply.Value = kv.kvps[args.Key]
		kv.kvps[args.Key] += args.Value
	}

	kv.lastAppendResult[args.ClientId] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvps = make(map[string]string)
	kv.processedCallCount = make(map[int64]int)
	kv.lastAppendResult = make(map[int64]string)

	return kv
}
