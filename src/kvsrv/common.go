package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key                       string
	Value                     string
	ClientId                  int64
	ClientSuccessfulCallCount int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key                       string
	ClientId                  int64
	ClientSuccessfulCallCount int
}

type GetReply struct {
	Value string
}
