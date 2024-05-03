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
	lastCallResult     map[int64]string // last call processed for a client
}

func (kv *KVServer) cleanup(clientId int64, result string) {
	kv.processedCallCount[clientId]++
	kv.lastCallResult[clientId] = result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.processedCallCount[args.ClientId] != args.ClientSuccessfulCallCount {
		reply.Value = kv.lastCallResult[args.ClientId]
		return
	}

	if _, ok := kv.kvps[args.Key]; !ok {
		reply.Value = ""
		kv.cleanup(args.ClientId, reply.Value)
		return
	}
	reply.Value = kv.kvps[args.Key]
	kv.cleanup(args.ClientId, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.processedCallCount[args.ClientId] != args.ClientSuccessfulCallCount {
		reply.Value = kv.lastCallResult[args.ClientId]
		return
	}

	kv.kvps[args.Key] = args.Value
	reply.Value = args.Value
	kv.cleanup(args.ClientId, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.processedCallCount[args.ClientId] != args.ClientSuccessfulCallCount {
		reply.Value = kv.lastCallResult[args.ClientId]
		return
	}

	if _, ok := kv.kvps[args.Key]; !ok {
		reply.Value = ""
		kv.kvps[args.Key] = args.Value
		kv.cleanup(args.ClientId, reply.Value)
	} else {
		reply.Value = kv.kvps[args.Key]
		kv.kvps[args.Key] += args.Value
		kv.cleanup(args.ClientId, reply.Value)
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvps = make(map[string]string)
	kv.processedCallCount = make(map[int64]int)
	kv.lastCallResult = make(map[int64]string)

	return kv
}
