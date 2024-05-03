package kvsrv

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server              *labrpc.ClientEnd
	id                  int64
	successfulCallCount int // Number of successful calls made by the client
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.id = nrand()
	ck.successfulCallCount = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		key,
		ck.id,
		ck.successfulCallCount,
	}
	reply := GetReply{}

	ok := false

	// might need to sleep here ?
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		key,
		value,
		ck.id,
		ck.successfulCallCount,
	}
	reply := PutAppendReply{}

	ok := false

	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}

	ck.successfulCallCount++
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
