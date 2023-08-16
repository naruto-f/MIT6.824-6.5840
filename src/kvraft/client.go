package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastServerId    int64
	mu              sync.Mutex
	curRequestSeqId int64
	clientId        int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// fmt.Println("Client restart ")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.curRequestSeqId = 0
	ck.lastServerId = -1
	return ck
}

// fhetch te current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{}
	reply := GetReply{}
	args.ClientId = ck.clientId
	args.RequestSeqId = ck.curRequestSeqId
	ck.curRequestSeqId++
	args.Key = key

	if ck.lastServerId == -1 {
		//ck.lastServerId = nrand() % int64(len(ck.servers))
		ck.lastServerId = 0
	}

	for {
		if ck.lastServerId == int64(len(ck.servers)) {
			ck.lastServerId = 0
		}

		reply = GetReply{}

		ok := ck.servers[ck.lastServerId].Call("KVServer.Get", &args, &reply)
		if !ok {
			println("Get Rpc send err")
			ck.lastServerId++
			continue
		}

		if reply.Err == ErrWrongLeader {
			println("Get Wrong leader")
			ck.lastServerId++
			continue
		} else if reply.Err == ErrAlreadySend {
			return ""
		}

		break
	}

	if reply.Err == ErrNoKey {
		fmt.Println("Get Fail, key = ", key, " no key!")
		return ""
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.ClientId = ck.clientId
	args.RequestSeqId = ck.curRequestSeqId
	ck.curRequestSeqId++
	args.Key = key
	args.Value = value
	args.Op = op
	if op == "Put" {
		fmt.Println("Put, key = ", key, ", value = ", value, " args.ClientId = ", args.ClientId, " ck.clientId = ", ck.clientId, "ck.curRequestSeqId = ", ck.curRequestSeqId)
	} else {
		fmt.Println("Append, key = ", key, ", value = ", value, " args.ClientId = ", args.ClientId, " ck.clientId = ", ck.clientId, "ck.curRequestSeqId = ", ck.curRequestSeqId)
	}

	if ck.lastServerId == -1 {
		// ck.lastServerId = nrand() % int64(len(ck.servers))
		ck.lastServerId = 0
	}

	for {
		if ck.lastServerId == int64(len(ck.servers)) {
			ck.lastServerId = 0
		}

		reply = PutAppendReply{}

		ok := ck.servers[ck.lastServerId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			println("Put Rpc send err")
			ck.lastServerId++
			continue
		}

		if reply.Err == ErrWrongLeader {
			println("Put Wrong leader")
			ck.lastServerId++
			continue
		} else if reply.Err == ErrAlreadySend {
			return
		}

		break
	}

	fmt.Println("Put Or Append Successful", ", key = ", key)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
