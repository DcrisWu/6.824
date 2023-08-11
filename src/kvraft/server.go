package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	ClientId    int64  // Client的Id
	RequestId   int    // 请求的Id
	CommandType string // 操作类型
	Key         string
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// map作为database
	db        map[string]string // 模拟数据库
	waitCh    map[int]chan Op   // raft中对应index的Op的channel
	lastApply map[int64]int     // 针对特定一个client的最后一次请求的序号
}

const TimeOutInterval = 300 * time.Millisecond

// CallSnapshot 对于log，需要判断是否需要进行快照
func (kv *KVServer) CallSnapshot(raftIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.lastApply)
	snapshot := w.Bytes()
	kv.rf.Snapshot(raftIndex, snapshot)
}

// 判断是不是Duplicate
func (kv *KVServer) ifDuplicate(clientId int64, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastApplyIndex, exist := kv.lastApply[clientId]
	// 如果不存在这个键值对，就创建一个
	if !exist {
		return false
	}
	// 如果当前请求的id <= 这个client的lastApply，就返回true
	return requestId <= lastApplyIndex
}

// 不断apply Op
func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		message := <-kv.applyCh
		// 如果这是Log
		if message.CommandValid {
			kv.GetCommandFormat(message)
			// 对于Put/Append这类改变database的操作，运行到这一步已经结束，就可以进行持久化
			// 如果需要进行快照
			kv.mu.Lock()
			// 如果maxraftstate != -1，就判断是否需要进行快照
			if kv.maxraftstate != -1 && kv.rf.GetPersistSize() >= kv.maxraftstate {
				kv.CallSnapshot(message.CommandIndex)
			}
			kv.mu.Unlock()
		} else if message.SnapshotValid {
			// 如果是快照的话，安装快照
			kv.GetSnapshotFormat(message)
		}
	}
}

func (kv *KVServer) GetSnapshotFormat(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := msg.Snapshot
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	// 调用接口，判断是否可以安装快照
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, snapshot) {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var db map[string]string
		var lastApply map[int64]int
		if d.Decode(&db) != nil || d.Decode(&lastApply) != nil {
			DPrintf("decode error")
		} else {
			kv.db = db
			kv.lastApply = lastApply
		}
	}
}

func (kv *KVServer) GetCommandFormat(msg raft.ApplyMsg) {
	command := msg.Command.(Op)
	// 如果是Duplicate，就需要判断是Append还是Put，再决定KVServer是否执行
	if !kv.ifDuplicate(command.ClientId, command.RequestId) {
		if command.CommandType == "Put" {
			kv.doPut(command)
		}
		if command.CommandType == "Append" {
			kv.doAppend(command)
		}
	}
	kv.sendOpToCh(msg.CommandIndex, command)
}

// 通关channel发送Op
func (kv *KVServer) sendOpToCh(index int, operation Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitCh[index]
	if exist {
		ch <- operation
	}
}

func (kv *KVServer) doGet(operation Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.db[operation.Key]
	seq, isHas := kv.lastApply[operation.ClientId]
	if !isHas {
		kv.lastApply[operation.ClientId] = operation.RequestId
	} else {
		kv.lastApply[operation.ClientId] = raft.Max(seq, operation.RequestId)
	}
	if !exist {
		return "", false
	} else {
		return value, true
	}
}

func (kv *KVServer) doPut(operation Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[operation.Key] = operation.Value
	kv.lastApply[operation.ClientId] = operation.RequestId
}

func (kv *KVServer) doAppend(operation Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.db[operation.Key]
	if !exist {
		kv.db[operation.Key] = operation.Value
	} else {
		kv.db[operation.Key] += operation.Value
	}
	kv.lastApply[operation.ClientId] = operation.RequestId
}

// Get Get操作不需要考虑是不是Duplicate
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 创建一个op
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, CommandType: "Get", Key: args.Key}
	// 向raft中提交一个log
	//DPrintf("server[%d] append log to raft", kv.me)
	index, _, isLeader := kv.rf.Start(op)
	// 如果当前server不是leader || KVServer已经 killed
	if kv.killed() || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("server[%v] start command %v\n", kv.me, args)
	kv.mu.Lock()
	// 创建一个等待回复的channel
	waitApplyCh, exist := kv.waitCh[index]
	if !exist {
		kv.waitCh[index] = make(chan Op, 1)
		waitApplyCh = kv.waitCh[index]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Duration(TimeOutInterval)):
		// TimeOut可能是因为raft在一个minority，但已经commit过的信息一定是正确的，
		// 所以只要它认为自己是leader，就可以回复
		_, isLeader := kv.rf.GetState()
		if kv.ifDuplicate(op.ClientId, op.RequestId) && isLeader {
			value, exist := kv.doGet(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case commitOp := <-waitApplyCh:
		//_, isLeader := kv.rf.GetState()
		if commitOp.RequestId == args.RequestId && commitOp.ClientId == args.ClientId {
			value, exist := kv.doGet(commitOp)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	// 延迟释放waitApplyCh
	go func(index int) {
		kv.mu.Lock()
		//close(waitApplyCh)
		delete(kv.waitCh, index)
		DPrintf("server[%d] send reply, Err :%v", kv.me, reply.Err)
		kv.mu.Unlock()
	}(index)
}

// PutAppend 进行Put或者Append操作
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 创建一个op
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, CommandType: args.Op, Key: args.Key, Value: args.Value}
	// 向raft中提交一个log
	index, _, isLeader := kv.rf.Start(op)
	// 如果当前server不是leader || KVServer已经 killed
	if kv.killed() || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("server[%v] start command %v\n", kv.me, args)
	kv.mu.Lock()
	// 创建一个等待回复的channel
	waitApplyCh, exist := kv.waitCh[index]
	if !exist {
		kv.waitCh[index] = make(chan Op, 1)
		waitApplyCh = kv.waitCh[index]
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Duration(TimeOutInterval)):
		_, isLeader := kv.rf.GetState()
		if kv.ifDuplicate(op.ClientId, op.RequestId) && isLeader {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case commitOp := <-waitApplyCh:
		if commitOp.RequestId == args.RequestId && commitOp.ClientId == args.ClientId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	// 延迟释放waitApplyCh
	go func(index int) {
		kv.mu.Lock()
		//close(waitApplyCh)
		delete(kv.waitCh, index)
		DPrintf("server[%d] send reply, Err :%v", kv.me, reply.Err)
		DPrintf("server[%d] after %v, key :%v, value :%v", kv.me, op.CommandType, args.Key, kv.db[args.Key])
		kv.mu.Unlock()
	}(index)
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
	kv.db = make(map[string]string)
	kv.waitCh = make(map[int]chan Op)
	kv.lastApply = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// 不断apply从raft传来的Log
	go kv.applyLoop()

	return kv
}
