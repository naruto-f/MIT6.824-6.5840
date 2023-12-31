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
	"6.5840/labgob"
	"bytes"
	"crypto/rand"
	"math"
	"math/big"

	//	"bytes"
	// "math/rand"
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
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's
	applyMu           sync.Mutex          // Lock to searlize start
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	snapshot          []byte              // current snapshot
	lastIncludedTerm  int
	lastIncludedIndex int
	me                int   // this peer's index into peers[]
	dead              int32 // set by Kill()
	cond              *sync.Cond
	startCond         *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	votedInThisTerm bool
	restoreSuccess  bool
	log             []LogEntry
	heartBeat       time.Duration
	electionTime    time.Time
	applying        atomic.Int32

	commitIndex       int
	lastApplied       int
	applyCh           chan ApplyMsg
	snapshotApplyFlag bool

	nextIndex  []int
	matchIndex []int

	// 0 for leader, 1 for candidate, 2 for follower
	role               int32
	appendedInThisTick bool
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	logLen := len(rf.log)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(logLen)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	for i := 0; i < len(rf.log); i++ {
		e.Encode(rf.log[i])
	}
	raftstate := w.Bytes()
	if rf.snapshot != nil && len(rf.snapshot) != 0 {
		rf.persister.Save(raftstate, rf.snapshot)
	} else {
		rf.persister.Save(raftstate, nil)
	}
	// Debug(dInfo, "S%d Persist, currentTerm = %d, voteFor = %d, logLen = %d!", rf.me, rf.currentTerm, rf.votedFor, logLen)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.restoreSuccess = false
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var logLen int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&logLen) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		Debug(dInfo, "readPersist fail!")
		rf.restoreSuccess = false
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
	}
	Debug(dInfo, "S%d readPersist, currentTerm = %d, voteFor = %d, logLen = %d!", rf.me, currentTerm, voteFor, logLen)

	log := make([]LogEntry, logLen)
	for i := 0; i < logLen; i++ {
		entry := LogEntry{}
		if d.Decode(&entry) != nil {
			Debug(dInfo, "readPersist log entry fail!")
			rf.restoreSuccess = false
			return
		} else {
			Debug(dInfo, "S%d, entry:  Term = %d, Index = %d!", rf.me, entry.Term, entry.Index)
			// log = append(log, entry)
			log[i] = entry
		}
	}
	rf.log = make([]LogEntry, logLen)
	copy(rf.log, log)
	rf.snapshot = rf.persister.ReadSnapshot()
	Debug(dInfo, "S%d, logLen = %d, rf.logLen = %d!", rf.me, len(log), len(rf.log))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != 0 {
		return
	}

	if index < rf.lastIncludedIndex || (index == rf.lastIncludedIndex && (len(rf.log) == 0 || index < rf.log[0].Index)) {
		return
	}

	rf.lastIncludedIndex = rf.log[index-rf.log[0].Index].Index
	rf.lastIncludedTerm = rf.log[index-rf.log[0].Index].Term
	rf.log = rf.log[index-rf.log[0].Index+1:]
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)

	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}

	rf.persist()
	Debug(dSnap, "S%d, take snapshot lastIncludedIndex = %d, lastIncludedTerm = %d!", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term              int
	CandidateId       int
	LastLogIndex      int
	LastLogTerm       int
	LastIncludedIndex int
	LastIncludedTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	// Your data here (2D).
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	LeaderCommit      int
	Data              []byte
	Logs              []LogEntry
}

type InstallSnapshotReply struct {
	// Your data here (2D).
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommit      int
	InconsistentRetry bool
	TermIndexInfo     [][]int
	LastIncludedIndex int
	LastIncludedTerm  int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term              int
	Success           bool
	Inconsistent      bool
	TermNeedSync      int
	SnapshotNeed      bool
	LastIncludedIndex int
	LastIncludedTerm  int
}

type CommitEntriesArgs struct {
	Term int
}

type CommitEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func min(left int, right int) int {
	if left <= right {
		return left
	}
	return right
}

func max(left int, right int) int {
	if left <= right {
		return right
	}
	return left
}

func (rf *Raft) ReadRaftstateSize() int {
	println("read snapshot size...")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	println("read snapshot size successful")
	return len(rf.persister.ReadRaftState())
}

func (rf *Raft) ReadSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return clone(rf.persister.ReadSnapshot())
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if reply.Term > args.Term {
		rf.persist()
		rf.mu.Unlock()
		return
	}

	Debug(dSnap, "S%d, recv snapshot from S%d!", rf.me, args.LeaderId)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.role = 2
		rf.votedFor = -1
	}

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		reply.Success = false
		rf.persist()
		rf.mu.Unlock()
		return
	}

	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex

	rf.log = make([]LogEntry, len(args.Logs))
	copy(rf.log, args.Logs)

	if len(args.Logs) != 0 {
		rf.commitIndex = min(args.Logs[len(args.Logs)-1].Index, args.LeaderCommit)
		rf.lastApplied = min(rf.lastIncludedIndex, rf.commitIndex)
	} else {
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}

		if rf.commitIndex < rf.lastIncludedIndex {
			rf.commitIndex = rf.lastIncludedIndex
		}
	}

	rf.persist()
	rf.resetElectionTimer()

	reply.Success = true
	rf.snapshotApplyFlag = true
	rf.cond.Signal()
	rf.mu.Unlock()

	//Debug(dSnap, "S%d, install snapshot from S%d successful!", rf.me, args.LeaderId)
}

func (rf *Raft) CommitEntries(args *CommitEntriesArgs, reply *CommitEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.persist()
		return
	}

	if args.Term > rf.currentTerm {
		// Debug(dLog, "S%d convert to Follower, cause S%d term larger!", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		if rf.role != 2 {
			rf.role = 2
			rf.votedFor = -1
		}
		rf.persist()
		return
	}

	if rf.lastApplied < rf.commitIndex {
		Debug(dLog, "S%d(Follower), awaken apply thread!", rf.me)
		rf.cond.Signal()
	}
	rf.persist()
	reply.Success = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dTimer, "S%d recv heartbeat or new log from S%d(Leader)!", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Inconsistent = false
	reply.LastIncludedIndex = rf.lastIncludedIndex
	reply.LastIncludedTerm = rf.lastIncludedTerm
	if args.Term < rf.currentTerm {
		rf.persist()
		return
	}

	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.role = 2
		rf.votedFor = -1
		rf.persist()
		return
	}

	logLen := len(rf.log)
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		Debug(dLog, "S%d, lastIncludedIndex Inconsistent 1!", rf.me)
		if (logLen > 0 && rf.log[logLen-1].Index > args.PrevLogIndex) || (logLen == 0 && rf.lastIncludedIndex >= args.PrevLogIndex) {
			reply.SnapshotNeed = true
			reply.Inconsistent = true
		} else {
			reply.Success = true
		}
		//reply.Success = true
		rf.persist()
		return
	}

	//rf.resetElectionTimer()
	//|| (len(rf.log) == 0 || rf.log[len(rf.log)-1].Index <= args.LastIncludedIndex)
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		Debug(dLog, "S%d, lastIncludedIndex Inconsistent 2!", rf.me)
		reply.SnapshotNeed = true
		reply.Inconsistent = true
		rf.persist()
		return
	}

	if len(args.Entries) > 0 {
		if logLen > 0 {
			if rf.log[logLen-1].Index >= args.Entries[len(args.Entries)-1].Index && rf.log[logLen-1].Term >= args.Entries[len(args.Entries)-1].Term {
				reply.Success = true
				rf.persist()
				return
			}
		} else {
			if rf.lastIncludedIndex >= args.Entries[len(args.Entries)-1].Index && rf.lastIncludedTerm >= args.Entries[len(args.Entries)-1].Term {
				reply.Success = false
				rf.persist()
				return
			}
		}
	}

	if args.InconsistentRetry {
		Debug(dLog, "InconsistentRetry: S%d(Follower) need append %d logs!", rf.me, len(args.Entries))
	} else if args.PrevLogIndex != 0 {
		Debug(dInfo, "S%d(Follower) rf.lastIncludedIndex = %d, args.LastIncludedIndex = %d, rf.lastIncludedTerm = %d, args.LastIncludedTerm = %d!", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedTerm)
		if rf.lastIncludedIndex < args.LastIncludedIndex || (rf.lastIncludedIndex == args.LastIncludedIndex && rf.lastIncludedTerm != args.LastIncludedTerm) {
			reply.SnapshotNeed = true
			reply.Inconsistent = true
			rf.persist()
			return
		} else if rf.lastIncludedIndex == args.LastIncludedIndex && rf.lastIncludedIndex > args.PrevLogIndex {
			reply.Inconsistent = true
			rf.persist()
			return
		}

		if logLen == 0 {
			if rf.lastIncludedIndex == args.PrevLogIndex && rf.lastIncludedTerm == args.PrevLogTerm {

			} else {
				reply.Inconsistent = true
				rf.persist()
				return
			}
		} else {
			// println("logLen = %d, args.PrevLogIndex = %d, rf.log[0].Index = %d!", logLen, args.PrevLogIndex, rf.log[0].Index)
			Debug(dLog, "S%d, logLen = %d, args.PrevLogIndex = %d, rf.log[logLen-1].Index = %d, args.PrevLogIndex-rf.log[0].Index = %d, args.PrevLogTerm = %d, rf.log[0].Index = %d, rf.log[0].Term = %d!", rf.me, logLen, args.PrevLogIndex, rf.log[logLen-1].Index, args.PrevLogIndex-rf.log[0].Index, args.PrevLogTerm, rf.log[0].Index, rf.log[0].Term)
			if rf.log[logLen-1].Index+1 <= args.PrevLogIndex || (args.PrevLogIndex >= rf.log[0].Index && rf.log[args.PrevLogIndex-rf.log[0].Index].Term != args.PrevLogTerm) {
				if args.TermIndexInfo != nil {
					Debug(dLog, "S%d(Follower) TermIndexInfo is not null!", rf.me)
					index := 1
					for ; index < len(args.TermIndexInfo); index++ {
						if args.TermIndexInfo[index][1]-rf.log[0].Index-1 >= len(rf.log) {
							break
						}

						flag := false
						for i := args.TermIndexInfo[index-1][1]; i < args.TermIndexInfo[index][1]; i++ {
							if rf.log[i-rf.log[0].Index].Term != args.TermIndexInfo[index][0] {
								flag = true
								break
							}
						}

						if flag {
							break
						}
					}

					reply.TermNeedSync = args.TermIndexInfo[index-1][0]
					reply.Inconsistent = true
				} else {
					Debug(dLog, "S%d(Follower) TermIndexInfo is null!", rf.me)
				}

				if reply.Inconsistent {
					rf.persist()
					return
				}
			}
		}
	}

	Debug(dSnap, "S%d, rf.lastIncludedIndex = %d, rf.lastIncludedTerm = %d!", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	Debug(dInfo, "S%d, logLen = %d, len(args.Entries) = %d, args.LeaderCommit = %d, rf.commitIndex = %d, rf.lastApplied = %d!", rf.me, len(rf.log), len(args.Entries), args.LeaderCommit, rf.commitIndex, rf.lastApplied)

	if len(args.Entries) > 0 {
		Debug(dLog, "S%d(Follower) need append %d logs!", rf.me, len(args.Entries))
		if logLen == 0 {
			Debug(dInfo, "S%d(Follower) has 0 logs Before append entry!", rf.me)
		} else {
			Debug(dInfo, "S%d(Follower) has %d logs Before append entry, and rf.log[len(rf.log)-1].Index = %d!", rf.me, len(rf.log), rf.log[len(rf.log)-1].Index)
		}

		if args.InconsistentRetry {
			if len(rf.log) != 0 {
				if len(rf.log) > 0 && rf.log[len(rf.log)-1].Index <= args.LastIncludedIndex {
					rf.log = rf.log[:0]
				} else {
					newTerm := args.Entries[0].Term
					i := 0
					for ; i < len(rf.log); i++ {
						if rf.log[i].Term == newTerm {
							break
						}
					}

					if i == len(rf.log) {
						rf.log = rf.log[:0]
					} else {
						rf.log = rf.log[:i]
					}
				}
			}

			rf.log = append(rf.log, args.Entries[0:]...)
			Debug(dInfo, "S%d(Follower) has %d logs After append entry, and rf.log[len(rf.log)-1].Index = %d!", rf.me, len(rf.log), rf.log[len(rf.log)-1].Index)
		} else {
			if logLen != 0 {
				if rf.log[len(rf.log)-1].Index > args.PrevLogIndex {
					if args.PrevLogIndex >= rf.log[0].Index {
						rf.log = rf.log[0 : args.PrevLogIndex-rf.log[0].Index+1]
					} else {
						rf.log = rf.log[0:0]
					}
				}
			}

			rf.log = append(rf.log, args.Entries[0:]...)
			Debug(dInfo, "S%d(Follower) has %d logs After append entry, and rf.log[len(rf.log)-1].Index = %d!", rf.me, len(rf.log), rf.log[len(rf.log)-1].Index)
		}
	}
	Debug(dInfo, "S%d, logLen = %d, len(args.Entries) = %d, args.LeaderCommit = %d, rf.commitIndex = %d, rf.lastApplied = %d!", rf.me, len(rf.log), len(args.Entries), args.LeaderCommit, rf.commitIndex, rf.lastApplied)

	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log) != 0 {
			Debug(dInfo, "S%d, rf.log[len(rf.log)-1].Index = %d!", rf.me, rf.log[len(rf.log)-1].Index)

			if args.LeaderCommit > rf.log[len(rf.log)-1].Index {
				rf.commitIndex = rf.log[len(rf.log)-1].Index
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		} else {
			rf.commitIndex = min(args.LeaderCommit, rf.lastIncludedIndex)
		}
	} else {
		rf.commitIndex = args.LeaderCommit
	}

	Debug(dInfo, "S%d, logLen = %d, len(args.Entries) = %d, args.LeaderCommit = %d, rf.commitIndex = %d, rf.lastApplied = %d!", rf.me, len(rf.log), len(args.Entries), args.LeaderCommit, rf.commitIndex, rf.lastApplied)
	if rf.lastApplied < rf.commitIndex {
		Debug(dLog, "S%d(Follower), awaken apply thread!", rf.me)
		rf.cond.Signal()
	}

	rf.persist()
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
		Debug(dVote, "S%d don't vote to S%d, cause S%d term smaller!", rf.me, args.CandidateId, args.CandidateId)
		rf.persist()
		return
	}

	if args.Term > rf.currentTerm {
		Debug(dVote, "S%d convert to Follower, cause S%d term larger!", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.role = 2
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	if rf.role != 2 {
		Debug(dVote, "S%d don't vote to S%d, cause S%d role is not follow!", rf.me, args.CandidateId, rf.me)
		rf.persist()
		return
	}

	logLen := len(rf.log)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		Debug(dVote, "S%d don't vote to S%d, cause 1", rf.me, args.CandidateId)
		if logLen == 0 {
			Debug(dVote, "S%d don't vote to S%d, cause 2", rf.me, args.CandidateId)
			if args.LastLogTerm > rf.lastIncludedTerm || (args.LastLogTerm == rf.lastIncludedTerm && args.LastLogIndex >= rf.lastIncludedIndex) {
				Debug(dVote, "S%d(Follower) vote to S%d!", rf.me, args.CandidateId)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.resetElectionTimer()
			}
		} else {
			Debug(dVote, "S%d don't vote to S%d, cause 3", rf.me, args.CandidateId)
			Debug(dVote, "S%d logLen = %d, rf.log[0].Index = %d, args.LastLogTerm = %d, rf.log[logLen-1].Term = %d, args.LastLogIndex = %d, rf.log[logLen-1].Index = %d", rf.me, logLen, rf.log[0].Index, args.LastLogTerm, rf.log[logLen-1].Term, args.LastLogIndex, rf.log[logLen-1].Index)
			if (logLen == 1 && rf.log[0].Index == 0) || args.LastLogTerm > rf.log[logLen-1].Term || (args.LastLogTerm == rf.log[logLen-1].Term && args.LastLogIndex >= rf.log[logLen-1].Index) {
				Debug(dVote, "S%d(Follower) vote to S%d!", rf.me, args.CandidateId)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.resetElectionTimer()
			}
		}
	}
	rf.persist()
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

func (rf *Raft) sendCommitEntries(server int, args *CommitEntriesArgs, reply *CommitEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.CommitEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	// rf.startMu.Lock()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	isLeader := rf.role == 0
	if isLeader != true {
		return 0, currentTerm, isLeader
	}

	// Your code here (2B).
	entry := LogEntry{}
	entry.Command = command
	entry.Term = rf.currentTerm
	if len(rf.log) == 0 {
		entry.Index = rf.lastIncludedIndex + 1
	} else {
		entry.Index = rf.log[len(rf.log)-1].Index + 1
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	Debug(dLog, "S%d Leader, client append a new log at index %d", rf.me, entry.Index)

	rf.LeaderHandler(false)
	return entry.Index, currentTerm, isLeader
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
	//source := rand.NewSource(time.Now().UnixNano()) // 使用当前的纳秒生成一个随机源，也就是随机种子
	//rander := rand.New(source)                      // 生成一个rand

	result, _ := rand.Int(rand.Reader, big.NewInt(550))
	ms := 250 + result.Int64()
	Debug(dTimer, "S%d, reset timer after %dms!", rf.me, ms)
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) caculateCommitIndex() {
	minIndex := math.MaxInt
	maxIndex := math.MinInt

	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		}

		if minIndex > rf.matchIndex[index] {
			minIndex = rf.matchIndex[index]
		}

		if maxIndex < rf.matchIndex[index] {
			maxIndex = rf.matchIndex[index]
		}
	}

	for index := maxIndex; index > rf.commitIndex; index-- {
		if len(rf.log) == 0 {
			break
		}

		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}

			if rf.matchIndex[j] >= index {
				count++
			}
		}

		if count >= len(rf.peers)/2+1 {
			rf.commitIndex = index
			break
		}
	}
}

func (rf *Raft) LeaderHandler(needAppend bool) bool {
	if rf.role != 0 {
		return false
	}

	// once := sync.Once{}
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	snapshot := make([]byte, len(rf.snapshot))
	copy(snapshot, rf.snapshot)
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	nextIndex := make([]int, len(rf.peers))
	log := make([]LogEntry, len(rf.log))
	var termAndIndexInfo [][]int
	copy(nextIndex, rf.nextIndex)
	copy(log, rf.log)
	logLen := len(rf.log)

	if logLen != 0 {
		curTerm := rf.log[0].Term
		for i := 0; i < len(rf.log); i++ {
			if curTerm != rf.log[i].Term {
				pair := make([]int, 2)
				pair[0] = curTerm
				pair[1] = rf.log[i].Index
				termAndIndexInfo = append(termAndIndexInfo, pair)
				curTerm = rf.log[i].Term
			}
		}
		termAndIndexInfo = append(termAndIndexInfo, []int{curTerm, rf.log[len(rf.log)-1].Index + 1})
	}

	idToTerm := make(map[int]int)
	for index := 0; index < len(rf.peers); index++ {
		if index != rf.me {
			if logLen != 0 && nextIndex[index]-1-rf.log[0].Index >= 0 && nextIndex[index]-1-rf.log[0].Index < len(rf.log) {
				idToTerm[index] = rf.log[nextIndex[index]-1-rf.log[0].Index].Term
			} else {
				idToTerm[index] = -1
			}
		}
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		id := i
		go func(serverId int) {
			args := AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = commitIndex
			args.PrevLogIndex = nextIndex[serverId] - 1
			args.TermIndexInfo = termAndIndexInfo
			args.LastIncludedIndex = lastIncludedIndex
			args.LastIncludedTerm = lastIncludedTerm
			args.InconsistentRetry = false
			if idToTerm[serverId] == -1 {
				args.PrevLogTerm = lastIncludedTerm
			} else {
				args.PrevLogTerm = idToTerm[serverId]
			}

			if logLen != 0 {
				Debug(dInfo, "S%d, logLen = %d, log[logLen-1].Index = %d, args.PrevLogIndex = %d, log[0].Index = %d!", rf.me, logLen, log[logLen-1].Index, args.PrevLogIndex, log[0].Index)
				if log[logLen-1].Index > args.PrevLogIndex && args.PrevLogIndex+1 >= log[0].Index {
					args.Entries = log[args.PrevLogIndex-log[0].Index+1:]
				} else {
					Debug(dInfo, "S%d, logLen = %d, but send no log!", rf.me, logLen)
				}
				Debug(dTimer, "S%d Leader, with appendNewLog", rf.me)
			} else {
				Debug(dInfo, "S%d, logLen = 0!", rf.me)
				Debug(dTimer, "S%d Leader, checking heartbeats", rf.me)
			}
			for {
				reply := AppendEntriesReply{}

				Debug(dLog, "S%d Leader, next is send to S%d...", rf.me, serverId)
				ok := rf.sendAppendEntries(serverId, &args, &reply)
				Debug(dLog, "S%d Leader, sending to S%d...", rf.me, serverId)
				rf.mu.Lock()
				if rf.role != 0 || currentTerm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if ok == false {
					Debug(dLog, "S%d Leader, send heartbeat to S%d Fail!", rf.me, serverId)
					rf.mu.Unlock()
					return
				}

				Debug(dLog, "S%d Leader, send heartbeat to S%d Success!", rf.me, serverId)

				if ok == true {
					if reply.Success == false {
						if reply.Inconsistent == true {
							args.InconsistentRetry = true
							if reply.SnapshotNeed {
								// rf.mu.Unlock()
								args := InstallSnapshotArgs{}
								args.Term = currentTerm
								args.LeaderId = rf.me
								// TODO: maybe has some problem.
								args.LastIncludedIndex = rf.lastIncludedIndex
								args.LastIncludedTerm = rf.lastIncludedTerm
								args.LeaderCommit = rf.commitIndex
								args.Data = make([]byte, len(rf.snapshot))
								args.Logs = make([]LogEntry, len(rf.log))
								copy(args.Logs, rf.log)
								copy(args.Data, rf.snapshot)

								reply := InstallSnapshotReply{}
								ok := rf.sendInstallSnapshot(serverId, &args, &reply)
								Debug(dSnap, "S%d, snapshot sending to S%d...!", rf.me, serverId)
								if ok == false {
									Debug(dSnap, "S%d, snapshot sending to S%d fail!", rf.me, serverId)
									rf.persist()
									rf.mu.Unlock()
									return
								}

								Debug(dSnap, "S%d, snapshot sending to S%d successful!", rf.me, serverId)

								if rf.role != 0 || currentTerm != rf.currentTerm {
									rf.persist()
									rf.mu.Unlock()
									return
								}

								if rf.currentTerm < reply.Term {
									Debug(dTerm, "S%d Leader, convert to Follower, cause S%d term larger!", rf.me, serverId)
									rf.currentTerm = reply.Term
									rf.role = 2
									rf.votedFor = -1
									rf.persist()
									rf.mu.Unlock()
									return
								}

								Debug(dSnap, "S%d, snapshot send to S%d successful!", rf.me, serverId)

								if reply.Success {
									if len(args.Logs) != 0 {
										rf.matchIndex[serverId] = max(args.Logs[len(args.Logs)-1].Index, rf.matchIndex[serverId])
										rf.nextIndex[serverId] = max(rf.matchIndex[serverId]+1, rf.nextIndex[serverId])
									} else {
										if rf.matchIndex[serverId] < rf.lastIncludedIndex {
											rf.matchIndex[serverId] = rf.lastIncludedIndex
										}

										if rf.nextIndex[serverId] < rf.lastIncludedIndex+1 {
											rf.nextIndex[serverId] = rf.lastIncludedIndex + 1
										}
									}

									rf.caculateCommitIndex()
									if rf.lastApplied < rf.commitIndex {
										rf.cond.Signal()
									}
								} else {

								}
								rf.persist()
								rf.mu.Unlock()
								return
							}

							if rf.matchIndex[serverId] < rf.lastIncludedIndex {
								rf.matchIndex[serverId] = rf.lastIncludedIndex
							}

							if rf.nextIndex[serverId] < rf.lastIncludedIndex+1 {
								rf.nextIndex[serverId] = rf.lastIncludedIndex + 1
							}

							args.Entries = make([]LogEntry, len(log))
							copy(args.Entries, log)

							rf.caculateCommitIndex()
							if rf.lastApplied < rf.commitIndex {
								rf.cond.Signal()
							}

							rf.mu.Unlock()
							continue
						} else {
							if rf.currentTerm < reply.Term {
								Debug(dTerm, "S%d Leader, convert to Follower, cause S%d term larger!", rf.me, serverId)
								rf.currentTerm = reply.Term
								rf.role = 2
								rf.votedFor = -1
							}

							rf.persist()
							rf.mu.Unlock()
						}

					} else {
						if len(args.Entries) > 0 {
							if args.Entries[len(args.Entries)-1].Index > rf.matchIndex[serverId] {
								rf.matchIndex[serverId] = args.Entries[len(args.Entries)-1].Index
								rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
							}
						}
						rf.caculateCommitIndex()

						if rf.lastApplied < rf.commitIndex {
							rf.cond.Signal()
						}

						rf.mu.Unlock()
					}
				}
				break
			}
		}(id)
	}

	return true
}

func (rf *Raft) CandidateHandler() {
	var count int32 = 1

	rf.role = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	currentTerm := rf.currentTerm
	flag := false
	logLen := len(rf.log)
	var LastLogTerm, LastLogIndex int
	if logLen >= 1 {
		LastLogTerm = rf.log[logLen-1].Term
		LastLogIndex = rf.log[logLen-1].Index
	} else {
		LastLogTerm = rf.lastIncludedTerm
		LastLogIndex = rf.lastIncludedIndex
	}
	rf.persist()
	rf.resetElectionTimer()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		id := i
		go func(serverId int) {
			args := RequestVoteArgs{}
			args.Term = currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = LastLogIndex
			args.LastLogTerm = LastLogTerm
			args.LastIncludedIndex = lastIncludedIndex
			args.LastIncludedTerm = lastIncludedTerm

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(serverId, &args, &reply)
			if ok == false {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != 1 || currentTerm != rf.currentTerm {
				return
			}

			if ok == true {
				if reply.VoteGranted == false {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.role = 2
						rf.votedFor = -1
						rf.persist()
						// rf.resetElectionTimer()
						Debug(dVote, "S%d Candidater, convert to Follower, cause S%d term larger!", rf.me, serverId)
					}
				} else {
					if !flag {
						count++
						Debug(dVote, "S%d Candidater, recv vote from S%d!", rf.me, serverId)
						if int(count) >= len(rf.peers)/2+1 {
							Debug(dVote, "S%d Candidater, convert to Leader!", rf.me)
							flag = true
							rf.role = 0
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}

								rf.nextIndex[i] = LastLogIndex + 1
								rf.matchIndex[i] = 0
							}
							rf.LeaderHandler(false)
						}
					}
				}
			}
		}(id)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		time.Sleep(rf.heartBeat)
		now := time.Now()
		Debug(dTimer, "S%d, ticker timeout!", rf.me)
		rf.mu.Lock()
		role := rf.role
		if role == 0 {
			Debug(dTrace, "S%d Leader, enter LeaderHandler!", rf.me)
			rf.LeaderHandler(false)
		} else {
			if now.After(rf.electionTime) {
				Debug(dTimer, "S%d Follower, election timeout, convert to Candidater!", rf.me)
				rf.role = 1
				rf.CandidateHandler()
			}
		}
		if rf.lastApplied < rf.commitIndex {
			rf.cond.Signal()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.cond.L.Lock()
		for rf.lastApplied >= rf.commitIndex {
			Debug(dInfo, "S%d, no log need apply, wait...", rf.me)
			rf.cond.Wait()
			Debug(dInfo, "S%d, someone awaken me!", rf.me)
		}

		logs := make([]ApplyMsg, 0)
		applyMsg := ApplyMsg{}
		if rf.snapshotApplyFlag {
			applyMsg.CommandValid = false
			applyMsg.SnapshotValid = true
			applyMsg.Snapshot = make([]byte, len(rf.snapshot))
			copy(applyMsg.Snapshot, rf.snapshot)
			applyMsg.SnapshotIndex = rf.lastIncludedIndex
			applyMsg.SnapshotTerm = rf.lastIncludedTerm
			logs = append(logs, applyMsg)
			rf.snapshotApplyFlag = false
		}

		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}

		if rf.commitIndex < rf.lastIncludedIndex {
			rf.commitIndex = rf.lastIncludedIndex
		}

		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{}
				msg.Command = rf.log[i-rf.log[0].Index].Command
				msg.CommandIndex = i
				msg.CommandValid = true
				logs = append(logs, msg)
			}
			rf.lastApplied = rf.commitIndex
			Debug(dLog, "S%d, log%d apply start", rf.me, rf.lastApplied)
			rf.mu.Unlock()
			for _, msg := range logs {
				rf.applyCh <- msg
			}
			Debug(dLog, "S%d, log%d apply successful", rf.me, rf.lastApplied)
		} else {
			rf.mu.Unlock()
		}
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
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	rf.startCond = sync.NewCond(&rf.mu)
	rf.appendedInThisTick = false
	rf.applying.Store(0)
	rf.snapshotApplyFlag = false

	// Your initialization code here (2A, 2B, 2C).
	rf.restoreSuccess = true
	rf.readPersist(persister.ReadRaftState())

	if rf.restoreSuccess == false {
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1)
		rf.log[0].Index = 0
		rf.log[0].Term = 0
		rf.log[0].Command = 0
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		rf.commitIndex = 0
		rf.lastApplied = 0
	} else {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}

	// rf.heartBeat = time.Duration(100)
	rf.heartBeat = 50 * time.Millisecond
	rf.resetElectionTimer()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		if i == rf.me {
			continue
		}

		if len(rf.log) > 0 {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
			rf.matchIndex[i] = 0
		} else {
			rf.nextIndex[i] = rf.lastIncludedIndex + 1
			rf.matchIndex[i] = 0
		}
	}

	rf.role = 2
	// initialize from state persisted before a crash

	// start ticker goroutine to start elections
	go rf.applier()
	go rf.ticker()

	return rf
}
