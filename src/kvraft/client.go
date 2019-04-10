package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader 	int
	clientId 	int
	seqNum 		int
}



func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var countMu sync.Mutex
var clientCount int

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.seqNum = 0

	countMu.Lock()
	ck.clientId = clientCount
	clientCount += 1
	countMu.Unlock()

	fmt.Printf("")

	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	ck.seqNum += 1
	DPrintf("clerk %d start get %d\n",ck.clientId,ck.seqNum)

	args := GetArgs{
		ClientId : ck.clientId,
		SeqNum : ck.seqNum,
		Key : key,
	}
	reply := GetReply{}
	ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)

	if !ok || reply.WrongLeader || reply.Err == ErrTimeOut {
		FindLeader:
		for {
			for i := 0; i < len(ck.servers); i++ {
				innerReply := GetReply{}
				ok := ck.servers[i].Call("KVServer.Get", &args, &innerReply)
				if ok && !innerReply.WrongLeader && innerReply.Err != ErrTimeOut {
					ck.lastLeader = i
					reply = innerReply
					break FindLeader
				}
			}
			time.Sleep(time.Millisecond * 1000)
			DPrintf("client %d retry op %d\n",ck.clientId,ck.seqNum)
		}
	}

	DPrintf("clerk %d end get %d, get %s\n",ck.clientId,ck.seqNum,reply.Value)
	if reply.Err == OK {
		return reply.Value
	}else{
		return ""
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqNum += 1
	DPrintf("clerk %d start putAppend %d\n",ck.clientId,ck.seqNum)

	args := PutAppendArgs{
		ClientId : ck.clientId,
		SeqNum : ck.seqNum,
		Key : key,
		Value : value,
		Op : op,
	}
	reply := PutAppendReply{}

	ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)

	if !ok || reply.WrongLeader || reply.Err == ErrTimeOut {
		FindLeader:
		for {
			for i := 0; i < len(ck.servers); i++ {
				innerReply := PutAppendReply{}
				ok := ck.servers[i].Call("KVServer.PutAppend", &args, &innerReply)
				if ok && !innerReply.WrongLeader && innerReply.Err != ErrTimeOut {
					ck.lastLeader = i
					reply = innerReply
					break FindLeader
				}
			}
			time.Sleep(time.Millisecond * 1000)
			DPrintf("client %d retry op %d\n",ck.clientId,ck.seqNum)
		}
	}
	DPrintf("clerk %d end putAppend %d\n",ck.clientId,ck.seqNum)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
