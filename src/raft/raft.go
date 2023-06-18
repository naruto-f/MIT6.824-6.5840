package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	cond      sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	votedInThisTerm bool
	log             []LogEntry
	heartBeat       time.Duration
	electionTime    time.Time

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// 0 for leader, 1 for candidate, 2 for follower
	role       int32
	connecting bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := false
	if rf.role == 0 {
		isleader = true
	}
	return rf.currentTerm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = 2
		rf.votedFor = -1
	}
	rf.resetElectionTimer()
	rf.connecting = true
	reply.Success = true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = 2
		rf.votedFor = -1
	}

	if rf.role != 2 {
		return
	}

	//if rf.role == 0 || rf.role == 1 {
	//	return
	//}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//if args.LastLogTerm >= rf.log[len(rf.log)-1].term && args.LastLogIndex >= len(rf.log) {
		rf.connecting = true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		return
		//}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimer() {
	source := rand.NewSource(time.Now().UnixNano()) // 使用当前的纳秒生成一个随机源，也就是随机种子
	rander := rand.New(source)                      // 生成一个rand
	ms := 400 + (rander.Int63() % 200)
	rf.electionTime = time.Now()
	rf.electionTime.Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) LeaderHandler() {
	// defer rf.mu.Unlock()
	// wg := sync.WaitGroup{}
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.role != 0 {
		return
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		id := i
		currentTerm := rf.currentTerm
		// wg.Add(1)
		go func(serverId int) {
			args := AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(serverId, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != 0 || currentTerm != rf.currentTerm {
				return
			}

			if ok == true {
				if reply.Success == false && rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					atomic.StoreInt32(&rf.role, 2)
					rf.connecting = true
					rf.votedFor = -1
					rf.resetElectionTimer()
				}
			}
			// wg.Done()
		}(id)
	}

	// wg.Wait()
}

func (rf *Raft) CandidateHandler() {
	// defer rf.mu.Unlock()
	wg := sync.WaitGroup{}
	var count int32 = 1
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.role != 1 {
		rf.mu.Unlock()
		return
	}

	if rf.votedFor == -1 || rf.votedFor == rf.me {
		rf.votedFor = rf.me
	} else {
		rf.mu.Unlock()
		return
	}

	rf.resetElectionTimer()
	rf.role = 1
	rf.currentTerm++
	currentTerm := rf.currentTerm
	logLen := len(rf.log)
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		id := i
		wg.Add(1)
		go func(serverId int) {
			defer wg.Done()
			args := RequestVoteArgs{}
			args.Term = currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = logLen
			if args.LastLogIndex == 0 {
				args.LastLogTerm = 0
			} else {
				args.LastLogTerm = rf.log[args.LastLogIndex].Term
			}

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != 1 || currentTerm != rf.currentTerm {
				return
			}

			if ok == true {
				if reply.VoteGranted == false {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						atomic.StoreInt32(&rf.role, 2)
						rf.connecting = true
						rf.votedFor = -1
						rf.resetElectionTimer()
					}
				} else {
					atomic.AddInt32(&count, 1)
				}
			}
		}(id)
	}

	wg.Wait()
	rf.mu.Lock()
	if rf.role == 1 && currentTerm == rf.currentTerm {
		if int(count) >= len(rf.peers)/2+1 {
			rf.role = 0
			rf.LeaderHandler()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		role := rf.role
		// rf.mu.Unlock()
		if role == 0 {
			rf.LeaderHandler()
		} else {
			if time.Now().After(rf.electionTime) {
				rf.role = 1
				go rf.CandidateHandler()
			}
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartBeat * time.Millisecond)
		//else if role == 1 {
		//	go rf.CandidateHandler()
		//	rf.mu.Unlock()
		//
		//	// rand.Rand.Seed(time.Now().UnixNano())
		//	//rander := rand.Rand{}
		//	source := rand.NewSource(time.Now().UnixNano()) // 使用当前的纳秒生成一个随机源，也就是随机种子
		//	rander := rand.New(source)                      // 生成一个rand
		//	ms := 150 + (rander.Int63() % 300)
		//	time.Sleep(time.Duration(ms) * time.Millisecond)
		//	time.Now().After()
		//} else {
		//	// rf.mu.Lock()
		//
		//	if rf.connecting == true {
		//		rf.connecting = false
		//		rf.mu.Unlock()
		//		source := rand.NewSource(time.Now().UnixNano()) // 使用当前的纳秒生成一个随机源，也就是随机种子
		//		rander := rand.New(source)                      // 生成一个rand
		//		//times := time.Now().UnixNano()
		//		// rand.Seed(times)
		//		ms := 150 + (rander.Int63() % 300)
		//		time.Sleep(time.Duration(ms) * time.Millisecond)
		//	} else {
		//		rf.role = 1
		//		rf.mu.Unlock()
		//	}
		//}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartBeat = time.Duration(100)
	rf.resetElectionTimer()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.role = 2
	rf.connecting = true
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
