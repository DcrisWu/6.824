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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []logEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// 快照相关变量
	snapshot          []byte // 存储快照
	lastIncludedIndex int    // 上一次进行快照的log下标
	lastIncludedTerm  int    // 上一次进行快照时候的term

	//volatile state
	//leaderId    int // leaderId in current term
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	lastVoted           time.Time // 上一次为其他 server 投票
	lastHeartBeat       time.Time // 上一次收到 heartBeat 的时间
	lastInstallSnapshot time.Time // 上一次进行快照的时间

	state   RaftState     //  judge if peer me is leader, candidate or follower
	applyCh chan ApplyMsg // 向server apply command

	//volatile state on leader
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

type logEntry struct {
	Term    int
	Command interface{}
}

type RaftState int

// raft的状态
const (
	leader RaftState = iota
	candidate
	follower
)

const (
	electionTimeouts  = 300                    // 选举超时
	heartBeatInterval = 200 * time.Millisecond // 心跳检测时间间隔
)

//// GetPersistSize 获取raft的log的长度
//func (rf *Raft) GetPersistSize() int {
//	return len(rf.persister.raftstate)
//}

func (rf *Raft) GetPersistSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.persister.raftstate)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	logData := w.Bytes()
	rf.persister.SaveRaftState(logData)
}

// for 2D, persist raft state and snapshot
func (rf *Raft) persistStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	logData := w.Bytes()
	rf.persister.SaveStateAndSnapshot(logData, rf.snapshot)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	//DPrintf("currentTerm error :%v", d.Decode(&currentTerm))
	//DPrintf("votedFor error :%v", d.Decode(votedFor))
	//DPrintf("log error :%v", d.Decode(&log))
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		//DPrintf("currentTerm :%v", currentTerm)
		//DPrintf("votedFor :%v", votedFor)
		//DPrintf("log :%v", log)
		DPrintf("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server[%d] is making snapshot now!!!!!!!!!!!!!!!!!!!!!!!!!!!!", rf.me)
	DPrintf("index :%d", index)
	// 如果想要在Raft server的快照前生成快照的话，直接返回
	if index <= rf.lastIncludedIndex {
		DPrintf("server[%d] has a newer snapshot whose last index :%d", rf.me, rf.lastIncludedIndex)
		return
	}
	if index > rf.lastIncludedIndex+len(rf.log)-1 {
		DPrintf("illegal snapshot request")
		return
	}
	//rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.log = append([]logEntry{}, rf.log[index-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[0].Term
	rf.snapshot = snapshot
	DPrintf("server[%d]'s snapshot has been made", rf.me)
	// 持久化日志
	rf.persistStateAndSnapshot()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// for 2A
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for leader to update itself
	VoteGranted bool // true if follower contained entry matching prevLogIndex and prevLogR term
}

type AppendEntryArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []logEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntryReply struct {
	//PrevLogIndex int  //thr first index of log entry in PrevLogTerm
	//PrevLogTerm  int  //term of prevLogIndex entry
	XTerm   int  // follower中与leader冲突的log对应的任期号，如果follower在对应位置没有log，那么XTerm会返回-1
	XIndex  int  // follower中对应任期号为XTerm的第一条log条目的index
	XLen    int  // 如果follower在对应位置没有log，那么XTerm会返回-1,XLen表示空白的Log槽位数
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("server[%d] receives server[%d] 's vote request.", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 如果candidate的term > currentTerm，说明这是一个更新的 term number
	if args.Term > rf.currentTerm {
		rf.votedFor = -1           // 在 args.term 期间，没有给过任何人投票
		rf.currentTerm = args.Term // 将raft.currentTerm设为最新的term
		rf.state = follower        // raft之前可能是leader或者candidate
	}

	// raft 是否没有在 args.currentTerm 任期内给其他 candidate 投过票
	condition1 := rf.votedFor == -1
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	//inLog := len(rf.log) - 1
	//DPrintf("server[%d] args.LastLogTerm: %v\n", rf.me, args.LastLogTerm)
	//DPrintf("server[%d] rf.log[inLog].Term :%v\n", rf.me, rf.log[inLog].Term)
	//DPrintf("server[%d]'s args.LastLogIndex :%d, inLog :%d", rf.me, args.LastLogIndex, inLog)
	condition2 := (args.LastLogTerm > rf.log[lastLogIndex-rf.lastIncludedIndex].Term) ||
		(args.LastLogTerm == rf.log[lastLogIndex-rf.lastIncludedIndex].Term && args.LastLogIndex >= lastLogIndex)
	//DPrintf("server[%d] condition1: %v && condition2: %v\n", rf.me, condition1, condition2)
	// 判断raft 是否应给给 candidate 投票
	if condition1 && condition2 {
		rf.votedFor = args.CandidateId
		rf.lastVoted = time.Now()
		reply.VoteGranted = true
		//DPrintf("server[%d] vote server[%d]", rf.me, args.CandidateId)
	} else {
		// 如果不满足其中一个条件，就不给candidate投票
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	//DPrintf("server[%d] send reply :%v", rf.me, reply)
	//DPrintf("------------------------------------------------------------------------")
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// raft 收到 AppendEntry 前可能是 candidate，收到合法的ae立刻转为follower
	rf.state = follower
	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	rf.persist()

	rf.lastHeartBeat = time.Now()
	reply.Term = rf.currentTerm
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	//2. Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 如果follower的log比leader的短
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = lastLogIndex
		return
	}
	// 如果 args 的 PrevLogIndex <= rf.commitIndex，说明是已经提交的log，一定匹配成功
	// 或者，如果在PrevIndex处的log的Term跟args的Term是一致的，也匹配成功
	if args.PrevLogIndex <= rf.commitIndex ||
		rf.lastIncludedIndex <= args.PrevLogIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm {
		reply.Success = true
		// 可能存在因为网络环境而延迟到达的ae，只需要删除冲突的ae，不冲突的ae不需要删除
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		beginIndex := Max(rf.commitIndex+1, args.PrevLogIndex+1) // 取最开始可能发生冲突的下标
		endIndex := Min(lastNewEntryIndex, lastLogIndex)         // 现有log的最大索引与leader中的最大索引的最小值
		// 在beginIndex和endIndex之间可能发生潜在的冲突
		diffItemIndex := endIndex + 1 // 冲突日志至少在这里开始发生
		for i := beginIndex; i <= endIndex; i++ {
			// 发生冲突，就将冲突的下标更新
			if args.Entries[i-args.PrevLogIndex-1].Term != rf.log[i-rf.lastIncludedIndex].Term {
				diffItemIndex = i
				break
			}
		}
		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)
		if diffItemIndex != endIndex+1 {
			rf.log = rf.log[:diffItemIndex-rf.lastIncludedIndex]
		}
		// 4. Append any new entries not already in the log
		if diffItemIndex-args.PrevLogIndex-1 <= len(args.Entries)-1 { // 起始索引小于最大索引，还有日志未加入本地日志
			rf.log = append(rf.log, args.Entries[diffItemIndex-args.PrevLogIndex-1:]...) // AppendEntries rule 4
		}
		// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		//prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, lastLogIndex)
		}
		rf.persist()
		return
	}
	// 如果在rf.commitIndex后,在PrevLogIndex处存在日志冲突
	if rf.lastIncludedIndex <= args.PrevLogIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		reply.XIndex = rf.commitIndex + 1 //XIndex至少 > commitIndex
		for i := args.PrevLogIndex; i >= rf.commitIndex+1 &&
			rf.log[i-rf.lastIncludedIndex].Term == rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term; i-- {
			reply.XIndex = i
		}
		rf.persist()
		return
	}
	DPrintf("error ae")
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server[%d] receive a snapshot from leader[%d]", rf.me, args.LeaderId)
	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// 如果遇到一个更高的leader，就转变为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = args.LeaderId
		rf.persist()
	}
	rf.lastInstallSnapshot = time.Now()
	reply.Term = rf.currentTerm
	logIndex := args.LastIncludedIndex - rf.lastIncludedIndex
	if logIndex < 0 {
		DPrintf("server[%d] has a newer snapshot", rf.me)
		return
	}
	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	if logIndex < len(rf.log) && rf.log[logIndex].Term == args.Term {
		return
	}
	// 7.Discard the entire log
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log = append([]logEntry{}, logEntry{Term: args.Term})
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	rf.commitIndex = rf.lastIncludedIndex
	rf.persistStateAndSnapshot()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果rf不是leader，直接返回false
	if rf.killed() || rf.state != leader {
		isLeader = false
		//DPrintf("server[%d] is not leader", rf.me)
		return index, term, isLeader
	}
	//DPrintf("server[%d] is leader", rf.me)
	// 如果rf是leader的话，就进行日志协商
	entry := logEntry{
		rf.currentTerm,
		command,
	}
	// 向log中写入entry
	//log.Printf("leader[%d] append entry to log goroutine[%d] ", rf.me, os.Getgid())
	rf.log = append(rf.log, entry)
	index = rf.lastIncludedIndex + len(rf.log) - 1
	term = rf.currentTerm
	DPrintf("leader[%d]'s log's length :%d", rf.me, rf.lastIncludedIndex+len(rf.log))
	//DPrintf("--------------------------------------------------------------------")
	//DPrintf("leader[%d]'s log :%v", rf.me, rf.log)
	//DPrintf"server[%d]: term in Start()", rf.me)
	go rf.agreementTask()
	rf.persist()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleepTime := time.Duration(rand.Intn(electionTimeouts)+electionTimeouts) * time.Millisecond
		//fmt.Printf("server[%d] 's sleep time : %v\n", rf.me, sleepTime)
		time.Sleep(sleepTime)
		// 上锁，检测整个 raft server 的状态
		rf.mu.Lock()
		// raft server 不是一个 leader
		// 如果经过electionTimeout时间睡眠之后，检测到上一次的 heart beat 是在 sleep 之前的，
		// 睡眠期间没有其他 server 请求投票选举
		// 说明选举超时，要重新选举新的leader
		if rf.state != leader && time.Since(rf.lastHeartBeat) > sleepTime && time.Since(rf.lastVoted) > sleepTime &&
			time.Since(rf.lastInstallSnapshot) > sleepTime {
			// becomes candidate, send vote request to peers
			rf.state = candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me // candidate给自己投票
			rf.lastVoted = time.Now()
			// 创建 request vote args
			lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				lastLogIndex,
				rf.log[lastLogIndex-rf.lastIncludedIndex].Term,
			}
			// 向其他服务器发送请求
			go rf.sendVoteTask(args)
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendVoteTask(args RequestVoteArgs) {
	ticket := 1
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		// 对每个服务器的投票请求都启用一个协程
		go func(index int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(index, &args, &reply)
			if !ok {
				//fmt.Printf("server[%d] call peers[%d] failed!\n", rf.me, index)
				return
			}
			//fmt.Printf("reply struct: %v\n", reply)
			// 上锁计票
			rf.mu.Lock()
			if rf.state != candidate {
				//fmt.Printf("server[%d] is not candidate anymore!\n", rf.me)
				rf.mu.Unlock()
				return
			}
			// 收到过期回复
			if args.Term != rf.currentTerm {
				DPrintf("server[%d] get old reply!", rf.me)
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				DPrintf("server[%d] in term %d receive ticket from server[%d]\n", rf.me, rf.currentTerm, index)
				ticket++
				DPrintf("server[%d]'s ticket = %d\n", rf.me, ticket)
				// 如果它赢得了半数以上的选举
				//fmt.Printf("peers number: %d\n", len(rf.peers))
				if ticket > len(rf.peers)/2 {
					rf.state = leader
					DPrintf("server[%d] in term[%d] is leader now!", rf.me, rf.currentTerm)
					DPrintf("----------------------------------------------------------------------")
					//DPrintf("server[%d] receive %d ticket", rf.me, ticket)
					// 更新转化为leader后的属性
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i, _ := range rf.nextIndex {
						rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log)
					}
					go rf.sendHearBeatTask()
					//go rf.agreementTask()
					//return
				}
			} else if reply.Term > rf.currentTerm {
				rf.state = follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
			rf.persist()
			rf.mu.Unlock()
		}(index, args)
	}
	//fmt.Printf("server[%d]'s ticket = %d\n", rf.me, ticket)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 进行日志协商
func (rf *Raft) agreementTask() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	peerNum := len(rf.peers)
	me := rf.me
	for i := 0; i < peerNum; i++ {
		if i == me {
			continue
		}
		go func(server int) {
			for !rf.killed() {
				rf.mu.Lock()
				// 如果不是leader，就直接返回
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					DPrintf("leader[%d] send snapshot to server[%d]", rf.me, server)
					data := append([]byte{}, rf.snapshot...)
					args := InstallSnapshotArgs{
						rf.currentTerm,
						rf.me,
						rf.lastIncludedIndex,
						rf.lastIncludedTerm,
						data,
					}
					reply := InstallSnapshotReply{}
					rf.mu.Unlock()
					ok := rf.sendInstallSnapshot(server, &args, &reply)
					rf.mu.Lock()
					if !ok {
						rf.mu.Unlock()
						return
					}
					if rf.state != leader {
						rf.mu.Unlock()
						return
					}
					if args.Term != rf.currentTerm {
						DPrintf("leader[%d] get old reply", rf.me)
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						rf.state = follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					}
					// 发送快照成功
					DPrintf("leader[%d]'s nextIndex[%d] before alter :%d", rf.me, server, rf.nextIndex[server])
					rf.matchIndex[server] = Max(rf.matchIndex[server], args.LastIncludedIndex)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					go rf.changeCommitIndex()
					rf.mu.Unlock()
				} else {
					DPrintf("leader[%d] send log entries to server[%d]", rf.me, server)
					entries := append([]logEntry{}, rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:]...)
					args := AppendEntryArgs{
						rf.currentTerm,
						rf.me,
						rf.nextIndex[server] - 1,
						rf.log[rf.nextIndex[server]-rf.lastIncludedIndex-1].Term,
						entries,
						rf.commitIndex,
					}
					reply := AppendEntryReply{}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					if !ok {
						rf.mu.Unlock()
						return
					}
					if rf.state != leader {
						rf.mu.Unlock()
						return
					}
					if args.Term != rf.currentTerm {
						DPrintf("get old reply")
						rf.mu.Unlock()
						return
					}
					// 如果协商成功
					if reply.Success {
						rf.matchIndex[server] = Max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						go rf.changeCommitIndex()
						rf.mu.Unlock()
						return
					}
					// 如果遇到更高的Term，回归follower
					if reply.Term > rf.currentTerm {
						rf.state = follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					}
					// 只是协商失败
					DPrintf("leader[%d]'s nextIndex[%d] before alter :%d", rf.me, server, rf.nextIndex[server])
					DPrintf("reply's XIndex : %d", reply.XIndex)
					if reply.XTerm == -1 {
						rf.nextIndex[server] = reply.XIndex + 1
					} else {
						rf.nextIndex[server] = reply.XIndex
					}
					DPrintf("leader[%d]'s nextIndex[%d] after alter :%d", rf.me, server, rf.nextIndex[server])
					rf.mu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}
}

// 更新commitIndex
func (rf *Raft) changeCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != leader {
		return
	}
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	rf.matchIndex[rf.me] = lastLogIndex
	match := append([]int{}, rf.matchIndex...)
	sort.Ints(match)
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	// 需要先判断i > rf.commitIndex
	// 因为可能出现的错误为：Leader crash后重连，changeCommitIndex()是每apply一个log或者同步一个log，就进行一次
	// 而rf.commitIndex是volatile，断电重连后，在达成majority前，中间值为0，
	// 如果不先判断i > rf.commitIndex，就会在数组中出现负数
	if i := match[len(rf.peers)>>1]; i > rf.commitIndex && rf.log[i-rf.lastIncludedIndex].Term == rf.currentTerm {
		//DPrintf("server[%d]'s old commitIndex :%d, new commitIndex :%d", rf.me, rf.commitIndex, i)
		rf.commitIndex = i
	}
	//DPrintf("leader[%d]'s commitIndex :%d", rf.me, rf.commitIndex)
}

// 把发送给follower的ae提交，同时通知其他的follower该log已经被提交
func (rf *Raft) applyTask() {
	for !rf.killed() {
		rf.mu.Lock()
		var msg ApplyMsg
		needApply := false
		// apply 0~13，apply至9时，就会触发快照，Leader会修剪log，快照完毕之后，会继续apply剩下的日志，
		// 此时直接用rf.log[i]显然就不对了，因为修剪日志操作会移动日志。
		// Hint中也说了要独立于日志位置的索引方案
		if rf.lastApplied < rf.commitIndex {
			needApply = true
			// 如果需要apply的log在snapshot之前，就选择apply snapshot
			if rf.lastApplied < rf.lastIncludedIndex {
				DPrintf("server[%d] apply snapshot to state machine", rf.me)
				rf.lastApplied = rf.lastIncludedIndex
				msg = ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
			} else {
				DPrintf("server[%d] apply log to state machine", rf.me)
				rf.lastApplied++
				msg = ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
					CommandIndex: rf.lastApplied,
				}
			}
		}
		rf.mu.Unlock()
		if needApply {
			rf.applyCh <- msg
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// rf 已经被选举为 leader，它现在需要向其他server定时发送heartBeat
func (rf *Raft) sendHearBeatTask() {
	//DPrintf("server[%d] in term[%d] heartBeatTask :%v", rf.me, rf.currentTerm, sendHeartBeatTime)
	// 通过发送空的ae来向其他server发送heartbeat
	for !rf.killed() {
		rf.mu.Lock()
		flag := rf.state == leader
		rf.mu.Unlock()
		// 不是leader了就不用发送心跳了
		if !flag {
			return
		}
		go rf.agreementTask()
		time.Sleep(heartBeatInterval)
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
	rf.log = []logEntry{}
	// 初始化时候塞一个空log进去
	initEntry := logEntry{Term: 0}
	rf.log = append(rf.log, initEntry)
	rf.commitIndex = 0
	rf.lastApplied = 0

	initSnapshot := make([]byte, 0)
	rf.snapshot = initSnapshot
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.state = follower
	rf.applyCh = applyCh

	//go rf.checkAlive()
	//DPrintf("%d initialed", rf.me)

	//DPrintf("server[%d]'s log：%v", rf.me, rf.log)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// 定期将log apply to machine state
	go rf.applyTask()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func Min(n1, n2 int) int {
	if n1 > n2 {
		return n2
	}
	return n1
}

func Max(n1, n2 int) int {
	if n1 > n2 {
		return n1
	}
	return n2
}
