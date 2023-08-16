package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId  int //0 for Get; 1 for Put; 2 for Append
	Key   string
	Value string

	ClientId     int64
	RequestSeqId int64
	//sync.Cond
}

type DuplicateTable struct {
	// ClientId   int64
	DoneResult map[int64]string
	MaxSeqId   int64
	// cond       *sync.Cond
}

type KVServer struct {
	mu   sync.Mutex
	cond *sync.Cond
	me   int
	// rf      *raft.Raft
	rf                     *raft.Raft
	applyCh                chan raft.ApplyMsg
	dead                   int32 // set by Kill()
	database               map[string]string
	needRedirectionRequest map[string]int
	duplicateTable         map[int64]*DuplicateTable
	conds                  map[string]*sync.Cond
	curExeCmdIndex         int
	checkRun               bool

	SnapshotTerm  int
	SnapshotIndex int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func max(left int64, right int64) int64 {
	if left >= right {
		return left
	}
	return right
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil {
		println("read raft snapshot")
		snapshot = kv.rf.ReadSnapshot()
	}

	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	d.Decode(&kv.SnapshotIndex)
	d.Decode(&kv.SnapshotTerm)

	var tableSize int
	var size1 uintptr
	var size2 uintptr

	kv.duplicateTable = make(map[int64]*DuplicateTable)
	d.Decode(&tableSize)
	for i := 0; i < tableSize; i++ {
		var seq int64
		var size int
		table := new(DuplicateTable)
		table.DoneResult = make(map[int64]string)
		d.Decode(&seq)
		d.Decode(&table.MaxSeqId)
		d.Decode(&size)
		size1 += unsafe.Sizeof(seq)
		for i := 0; i < size; i++ {
			var k int64
			var v string
			d.Decode(&k)
			d.Decode(&v)
			table.DoneResult[k] = v
		}
		size1 += unsafe.Sizeof(*table)
		kv.duplicateTable[seq] = table
	}

	kv.database = make(map[string]string)
	size2 = unsafe.Sizeof(kv.database)
	d.Decode(&kv.database)
	println("duplicateTable len is ", size1, " database len is ", size2)
}

func (kv *KVServer) checkSnapshotSize(index int) {
	size := kv.rf.ReadRaftstateSize()
	if size > kv.maxraftstate {
		println("Snapshot 1")
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		e.Encode(kv.SnapshotIndex)
		e.Encode(kv.SnapshotTerm)

		tableSize := len(kv.duplicateTable)
		e.Encode(tableSize)

		for k, v := range kv.duplicateTable {
			e.Encode(k)
			left := max(0, v.MaxSeqId-10)
			right := v.MaxSeqId
			e.Encode(v.MaxSeqId)

			if v.MaxSeqId == -1 {
				e.Encode(0)
			} else {
				e.Encode(right - left + 1)
			}

			if v.MaxSeqId != -1 {
				for i := left; i <= right; i++ {
					e.Encode(i)
					e.Encode(v.DoneResult[i])
				}
			}
		}

		e.Encode(kv.database)
		snapshot := w.Bytes()
		println("Snapshot len is ", len(snapshot))
		kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *KVServer) checkLeader() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()

		if kv.checkRun {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				if len(kv.conds) != 0 {
					kv.needRedirectionRequest = make(map[string]int)
					for k, _ := range kv.conds {
						kv.needRedirectionRequest[k] = 0
						kv.conds[k].Broadcast()
					}
				}
				kv.checkRun = false
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{}
	op.OpId = 0
	op.Key = args.Key
	op.ClientId = args.ClientId
	op.RequestSeqId = args.RequestSeqId
	reply.Err = OK

	_, _, isServer := kv.rf.Start(op)
	if !isServer {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	kv.checkRun = true

	table, ok := kv.duplicateTable[op.ClientId]
	if !ok {
		kv.duplicateTable[op.ClientId] = new(DuplicateTable)
		table = kv.duplicateTable[op.ClientId]
		table.DoneResult = make(map[int64]string)
		table.MaxSeqId = -1
	}

	if op.RequestSeqId <= kv.duplicateTable[op.ClientId].MaxSeqId {
		reply.Err = OK
		value, exist := kv.duplicateTable[op.ClientId].DoneResult[args.RequestSeqId]
		if !exist {
			v, ok := kv.database[op.Key]
			if ok {
				reply.Value = v
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = OK
			reply.Value = value
		}
		return
	}

	condKey := strconv.FormatInt(op.ClientId, 10) + ":" + strconv.FormatInt(op.RequestSeqId, 10)

	cond, exist := kv.conds[condKey]
	if !exist {
		cond = sync.NewCond(&kv.mu)
		kv.conds[condKey] = cond
		if kv.conds[condKey] == nil || cond == nil {
			println("cond = ", cond)
		}
	}

	cond.Wait()
	_, flag := kv.needRedirectionRequest[condKey]
	if flag {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	value, exist := kv.duplicateTable[op.ClientId].DoneResult[args.RequestSeqId]
	if !exist {
		v, ok := kv.database[op.Key]
		if ok {
			reply.Value = v
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = OK
		reply.Value = value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{}
	if args.Op == "Put" {
		op.OpId = 1
	} else {
		op.OpId = 2
	}

	op.Key = args.Key
	op.Value = args.Value
	op.ClientId = args.ClientId
	op.RequestSeqId = args.RequestSeqId
	reply.Err = OK

	condKey := strconv.FormatInt(op.ClientId, 10) + ":" + strconv.FormatInt(op.RequestSeqId, 10)

	_, _, isServer := kv.rf.Start(op)
	if !isServer {
		reply.Err = ErrWrongLeader
		return
	}

	kv.checkRun = true

	table, ok := kv.duplicateTable[op.ClientId]
	if !ok {
		kv.duplicateTable[op.ClientId] = new(DuplicateTable)
		table = kv.duplicateTable[op.ClientId]
		table.DoneResult = make(map[int64]string)
		kv.duplicateTable[op.ClientId].MaxSeqId = -1
		table.MaxSeqId = -1
	}

	if op.RequestSeqId <= kv.duplicateTable[op.ClientId].MaxSeqId {
		reply.Err = OK
		return
	}

	cond, exist := kv.conds[condKey]
	if !exist {
		cond = sync.NewCond(&kv.mu)
		kv.conds[condKey] = cond
		if kv.conds[condKey] == nil || cond == nil {
			println("cond = ", cond)
		}
	}

	cond.Wait()
	_, flag := kv.needRedirectionRequest[condKey]
	if flag {
		reply.Err = ErrWrongLeader
	}
}

// read applych and execute cmd that already commit
func (kv *KVServer) Execute() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.mu.Lock()
		condKey := ""
		if msg.CommandValid {
			op := msg.Command.(Op)

			condKey = strconv.FormatInt(op.ClientId, 10) + ":" + strconv.FormatInt(op.RequestSeqId, 10)
			println("Exe, condKey = ", condKey)

			table, exist := kv.duplicateTable[op.ClientId]
			if !exist {
				kv.duplicateTable[op.ClientId] = new(DuplicateTable)
				table = kv.duplicateTable[op.ClientId]
				table.DoneResult = make(map[int64]string)
				table.MaxSeqId = -1
			}

			if op.RequestSeqId <= kv.duplicateTable[op.ClientId].MaxSeqId {
				value, ok := kv.conds[condKey]
				if ok {
					value.Broadcast()
				}
			} else {
				if op.OpId == 1 {
					kv.database[op.Key] = op.Value
					kv.duplicateTable[op.ClientId].DoneResult[op.RequestSeqId] = "1"
				} else if op.OpId == 2 {
					kv.database[op.Key] = kv.database[op.Key] + op.Value
					kv.duplicateTable[op.ClientId].DoneResult[op.RequestSeqId] = "1"
				} else {
					println("read cmd, condKey = ", condKey)
					value, exist := kv.database[op.Key]
					if !exist {
						println("Key ", op.Key, " not exist, condKey = ", condKey)
					} else {
						println("Key ", op.Key, " exist, value = ", value, " condKey = ", condKey)
						kv.duplicateTable[op.ClientId].DoneResult[op.RequestSeqId] = value
					}
				}

				kv.duplicateTable[op.ClientId].MaxSeqId = max(kv.duplicateTable[op.ClientId].MaxSeqId, op.RequestSeqId)
			}

			if kv.maxraftstate != -1 {
				kv.checkSnapshotSize(msg.CommandIndex)
			}
			kv.curExeCmdIndex = msg.CommandIndex
		} else {
			if kv.maxraftstate != -1 {
				if kv.SnapshotIndex < msg.SnapshotIndex {
					kv.readSnapshot(msg.Snapshot)
				}
			}
			kv.mu.Unlock()
			continue
		}

		value, ok := kv.conds[condKey]
		if ok {
			println("Broadcast, condKey = ", condKey)
			value.Broadcast()
		}

		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	kv.conds = make(map[string]*sync.Cond)
	kv.curExeCmdIndex = 0
	kv.database = make(map[string]string)
	kv.duplicateTable = make(map[int64]*DuplicateTable)
	kv.SnapshotIndex = 0
	kv.SnapshotTerm = 0
	kv.checkRun = false
	if kv.maxraftstate != -1 {
		kv.readSnapshot(nil)
	}

	go kv.Execute()
	go kv.checkLeader()

	return kv
}
