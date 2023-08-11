package kvraft

import (
	"6.824/labrpc"
	time2 "time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64 // 当前client的id
	requestId    int   // 已经发送过的request的最新id
	recentLeader int   // 当前的raft leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk 初始化
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.recentLeader = ck.getNewLeader()
	timecost = make(chan time2.Time, 1)
	//DPrintf("time is now :%v", time2.Now())
	return ck
}

var timecost chan time2.Time

// 尝试获取新的id
func (ck *Clerk) getNewLeader() int {
	return (ck.recentLeader + 1) % len(ck.servers)
}

// 获取最新的Request id
func (ck *Clerk) getRequestId() int {
	res := ck.requestId
	ck.requestId++
	return res
}

// fetch the current value for a key.
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
	reqId := ck.getRequestId()
	//if reqId%100 == 0 {
	//	//DPrintf("reqId :%d", reqId)
	//	timecost <- time2.Now()
	//	//DPrintf("after")
	//}
	//DPrintf("requestId :%d", reqId)
	// You will have to modify this function.
	args := GetArgs{
		key,
		ck.clientId,
		reqId,
	}
	for {
		reply := GetReply{}
		DPrintf("[Clerk %d] try to Get %s", ck.clientId, key)
		ok := ck.sendGetRequest(ck.recentLeader, &args, &reply)
		// 没有发送成功就重发
		if !ok || reply.Err == ErrWrongLeader {
			DPrintf("client[%d] send a rpc failed", ck.clientId)
			ck.recentLeader = ck.getNewLeader()
			continue
		}
		//DPrintf("client[%d] gets a reply, err :%s", ck.clientId, reply.Err)
		//if reply.Err == TimeOut {
		//	DPrintf("client[%d] gets a reply, error :%s", ck.clientId, reply.Err)
		//	continue
		//}
		DPrintf("[Clerk %d] Get %s -> %s", ck.clientId, key, reply.Value)
		if reply.Err == ErrNoKey {
			DPrintf("client[%d] gets a reply, error :%s", ck.clientId, reply.Err)
			return ""
		}
		if reply.Err == OK {
			DPrintf("client[%d] gets a reply, error :%s", ck.clientId, reply.Err)
			return reply.Value
		}
	}
	//return ""
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
	reqId := ck.getRequestId()
	//if reqId%100 == 0 {
	//	timecost <- time2.Now()
	//}
	//DPrintf("requestId :%d", reqId)
	args := PutAppendArgs{
		key,
		value,
		op,
		ck.clientId,
		reqId,
	}
	for {
		reply := PutAppendReply{}
		DPrintf("[Clerk %d] %s key :%s value :%s", ck.clientId, op, key, value)
		ok := ck.sendPutAppendRequest(ck.recentLeader, &args, &reply)
		// 没有发送成功就重发
		if !ok || reply.Err == ErrWrongLeader {
			DPrintf("client[%d] send a rpc failed", ck.clientId)
			ck.recentLeader = ck.getNewLeader()
			continue
		}
		//DPrintf("client[%d] gets a reply, err :%s", ck.clientId, reply.Err)
		//if reply.Err == TimeOut {
		//	DPrintf("client[%d] gets a reply, error :%s", ck.clientId, reply.Err)
		//	continue
		//}
		//if reply.Err == ErrWrongLeader {
		//	DPrintf("client[%d] gets a reply, error :%s", ck.clientId, reply.Err)
		//	ck.recentLeader = ck.getNewLeader()
		//	continue
		//}
		if reply.Err == OK {
			DPrintf("client[%d] gets a reply, error :%s", ck.clientId, reply.Err)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 发送Get RPC请求
func (ck *Clerk) sendGetRequest(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

// 发送Get RPC请求
func (ck *Clerk) sendPutAppendRequest(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}
