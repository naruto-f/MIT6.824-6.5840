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
	mu        sync.Mutex          // Lock to protect shared access to this peer's
	applyMu   sync.Mutex          // Lock to searlize start
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	cond      *sync.Cond
	startCond *sync.Cond

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
	applyCh     chan ApplyMsg

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
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommit      int
	InconsistentRetry bool
	TermIndexInfo     [][]int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term         int
	Success      bool
	Inconsistent bool
	TermNeedSync int
}

type CommitEntriesArgs struct {
	Term int
	// Your data here (2A, 2B).
	//LeaderId     int
	//PrevLogIndex int
	//PrevLogTerm  int
	//Entries      []LogEntry
	//LeaderCommit int
}

type CommitEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) CommitEntries(args *CommitEntriesArgs, reply *CommitEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		// Debug(dLog, "S%d convert to Follower, cause S%d term larger!", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		if rf.role != 2 {
			rf.role = 2
			rf.votedFor = -1
		}
		return
	}

	if rf.lastApplied < rf.commitIndex {
		Debug(dLog, "S%d(Follower), awaken apply thread!", rf.me)
		rf.cond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dTimer, "S%d recv heartbeat or new log from S%d(Leader)!", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Inconsistent = false
	if args.Term < rf.currentTerm {
		return
	}

	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.role = 2
		rf.votedFor = -1
	}

	logLen := len(rf.log)
	if args.InconsistentRetry {
		Debug(dLog, "S%d(Follower) need append %d logs!", rf.me, len(args.Entries))
	} else if args.PrevLogIndex != 0 {
		if logLen <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			index := 1
			for ; index < len(args.TermIndexInfo); index++ {
				if args.TermIndexInfo[index][1] > len(rf.log) {
					break
				}

				flag := false
				for i := args.TermIndexInfo[index-1][1]; i < args.TermIndexInfo[index][1]; i++ {
					if rf.log[i].Term != args.TermIndexInfo[index][0] {
						flag = true
						break
					}
				}

				if flag {
					break
				}

			}

			reply.TermNeedSync = args.TermIndexInfo[index][0]
			reply.Inconsistent = true
			return
		}
	}

	if len(args.Entries) > 0 {
		Debug(dLog, "S%d(Follower) need append %d logs!", rf.me, len(args.Entries))

		if args.InconsistentRetry {
			newTerm := args.Entries[0].Term
			i := 0
			for ; i < len(rf.log); i++ {
				if rf.log[i].Term == newTerm {
					break
				}
			}

			rf.log = rf.log[:i]
			rf.log = append(rf.log, args.Entries[0:]...)
		} else {
			for i, entry := range args.Entries {
				if entry.Index >= logLen {
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}

				if rf.log[entry.Index].Term != entry.Term {
					rf.log = rf.log[:entry.Index]
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		// math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

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
		return
	}

	logLen := len(rf.log)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if logLen == 1 || args.LastLogTerm > rf.log[logLen-1].Term || (args.LastLogTerm == rf.log[logLen-1].Term && args.LastLogIndex >= logLen-1) {
			Debug(dVote, "S%d(Follower) vote to S%d!", rf.me, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
		}
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

func (rf *Raft) sendCommitEntries(server int, args *CommitEntriesArgs, reply *CommitEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.CommitEntries", args, reply)
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
	entry.Index = len(rf.log)
	rf.log = append(rf.log, entry)
	Debug(dLog, "S%d Leader, client append a new log at index %d", rf.me, entry.Index)

	rf.appendedInThisTick = true
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
	// time.Duration(ms)
	// rf.electionTime.Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) LeaderHandler(needAppend bool) bool {
	if rf.role != 0 {
		return false
	}

	// once := sync.Once{}
	agreementNum := 1
	sendNum := 1
	flag := false
	termChange := false
	currentTerm := rf.currentTerm
	commitIndex := len(rf.log) - 1
	// commitIndex := rf.commitIndex
	matchIndex := make([]int, len(rf.peers))
	log := make([]LogEntry, len(rf.log))
	var termAndIndexInfo [][]int
	copy(matchIndex, rf.matchIndex)
	copy(log, rf.log)
	curTerm := rf.log[0].Term
	var readyCommit []int
	for i := 0; i < len(rf.log); i++ {
		if curTerm != rf.log[i].Term {
			pair := make([]int, 2)
			pair[0] = curTerm
			pair[1] = i
			termAndIndexInfo = append(termAndIndexInfo, pair)
			curTerm = rf.log[i].Term
		}
	}
	termAndIndexInfo = append(termAndIndexInfo, []int{curTerm, len(rf.log)})

	idToTerm := make(map[int]int)
	for index := 0; index < len(rf.peers); index++ {
		if index != rf.me {
			idToTerm[index] = rf.log[matchIndex[index]].Term
		}
	}
	logLen := len(rf.log)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		id := i
		go func(serverId int) {
			appendNewLog := false
			args := AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = commitIndex
			args.PrevLogIndex = matchIndex[serverId]
			args.TermIndexInfo = termAndIndexInfo
			args.InconsistentRetry = false
			args.PrevLogTerm = idToTerm[serverId]

			if logLen != 1 && logLen > args.PrevLogIndex+1 {
				args.Entries = log[args.PrevLogIndex+1:]
				appendNewLog = true
				Debug(dTimer, "S%d Leader, with appendNewLog", rf.me)
			} else {
				appendNewLog = false
				Debug(dTimer, "S%d Leader, checking heartbeats", rf.me)
			}

			for {
				reply := AppendEntriesReply{}

				Debug(dLog, "S%d Leader, next is send to S%d...", rf.me, serverId)
				ok := rf.sendAppendEntries(serverId, &args, &reply)
				Debug(dLog, "S%d Leader, sending to S%d...", rf.me, serverId)
				rf.mu.Lock()
				if rf.role != 0 || currentTerm != rf.currentTerm {
					if needAppend {
						if !termChange {
							termChange = true
						}
					}

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
							for i := 0; i < len(termAndIndexInfo); i++ {
								if termAndIndexInfo[i][0] == reply.TermNeedSync {
									args.Entries = log[termAndIndexInfo[i-1][1]:]
									break
								}
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
							sendNum++
							if needAppend {
								if needAppend {
									if !termChange {
										termChange = true
									}
								}
							}
							rf.mu.Unlock()
						}

					} else {
						if appendNewLog {
							rf.matchIndex[serverId] = args.Entries[len(args.Entries)-1].Index
							rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

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
								if rf.log[index].Term != rf.currentTerm {
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

						agreementNum++
						sendNum++
						if needAppend {
							Debug(dLog, "S%d Leader, recv log append agree from S%d", rf.me, serverId)
							if !termChange {
								if agreementNum >= len(rf.peers)/2+1 {
									if flag {
										go func(serverId int) {
											args := CommitEntriesArgs{}
											reply := CommitEntriesReply{}

											args.Term = currentTerm

											ok := rf.sendCommitEntries(serverId, &args, &reply)
											if ok {
												rf.mu.Lock()
												defer rf.mu.Unlock()
												if reply.Term > currentTerm {
													rf.currentTerm = reply.Term
													rf.role = 2
													rf.votedFor = -1
												}
											}
										}(serverId)
									} else {
										flag = true
										readyCommit = append(readyCommit, id)
										rf.cond.Signal()
										for i := 0; i < len(readyCommit); i++ {
											if readyCommit[i] != rf.me {
												// wg.Add(1)
												id := readyCommit[i]
												go func(serverId int) {
													args := CommitEntriesArgs{}
													reply := CommitEntriesReply{}
													args.Term = currentTerm

													ok := rf.sendCommitEntries(serverId, &args, &reply)
													if ok {
														rf.mu.Lock()
														defer rf.mu.Unlock()
														if reply.Term > currentTerm {
															rf.currentTerm = reply.Term
															rf.role = 2
															rf.votedFor = -1
														}
													}
												}(id)
											}
										}

									}
								} else {
									readyCommit = append(readyCommit, serverId)
								}
							}
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
	currentTerm := rf.currentTerm
	flag := false
	logLen := len(rf.log)
	LastLogTerm := rf.log[logLen-1].Term
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
			args.LastLogIndex = logLen - 1
			args.LastLogTerm = LastLogTerm

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

								rf.nextIndex[i] = len(rf.log)
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
		// time.Sleep(100 * time.Millisecond)
		time.Sleep(rf.heartBeat)
		now := time.Now()
		Debug(dTimer, "S%d, ticker timeout!", rf.me)
		rf.mu.Lock()
		role := rf.role
		if role == 0 {
			Debug(dTrace, "S%d Leader, enter LeaderHandler!", rf.me)
			if rf.appendedInThisTick {
				rf.appendedInThisTick = false
				rf.LeaderHandler(true)
			} else {
				rf.LeaderHandler(false)
			}
			// rf.LeaderHandler(true)
		} else {
			if now.After(rf.electionTime) {
				Debug(dTimer, "S%d Follower, election timeout, convert to Candidater!", rf.me)
				rf.role = 1
				rf.CandidateHandler()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.cond.L.Lock()
	for rf.killed() == false {
		for rf.lastApplied >= rf.commitIndex {
			Debug(dInfo, "S%d, no log need apply, wait...", rf.me)
			rf.cond.Wait()
			Debug(dInfo, "S%d, someone awaken me!", rf.me)
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{}
			msg.Command = rf.log[rf.lastApplied].Command
			msg.CommandIndex = rf.lastApplied
			msg.CommandValid = true
			Debug(dLog, "S%d, log%d apply start", rf.me, rf.log[rf.lastApplied].Index)
			rf.applyCh <- msg
			Debug(dLog, "S%d, log%d apply successful", rf.me, rf.log[rf.lastApplied].Index)
		}
	}
	rf.cond.L.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0].Index = 0
	rf.log[0].Term = 0
	rf.log[0].Command = 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	// rf.heartBeat = time.Duration(100)
	rf.heartBeat = 100 * time.Millisecond
	rf.resetElectionTimer()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.role = 2
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
