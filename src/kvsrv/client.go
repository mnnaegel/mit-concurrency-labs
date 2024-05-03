package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		key,
	}
	reply := GetReply{}

	ok := ck.server.Call("KVServer.Get", &args, &reply)
	if !ok {
		panic("Server call failed")
	}
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		key,
		value,
	}
	reply := PutAppendReply{}

	ok := ck.server.Call("KVServer."+op, &args, &reply)
	if !ok {
		panic("RPC to the server failed for PutAppend")
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
