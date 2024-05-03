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
	mu   sync.Mutex
	kvps map[string]string
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
	kv.kvps[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.kvps[args.Key]; !ok {
		reply.Value = ""
		kv.kvps[args.Key] = args.Value
	} else {
		reply.Value = kv.kvps[args.Key]
		kv.kvps[args.Key] += args.Value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvps = make(map[string]string)
	return kv
}
