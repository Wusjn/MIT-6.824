package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.lastLeader = 0
	ck.seqNum = 0

	countMu.Lock()
	ck.clientId = clientCount
	clientCount += 1
	countMu.Unlock()

	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqNum += 1
	args := &QueryArgs{
		Num : num,
		ClientId : ck.clientId,
		SeqNum : ck.seqNum,
	}
	// Your code here.

	reply := QueryReply{}
	ok := ck.servers[ck.lastLeader].Call("ShardMaster.Query", args, &reply)
	if !ok || reply.WrongLeader || reply.Err == ErrTimeOut {
		FindLeader:
		for {
			// try each known server.
			for i, srv := range ck.servers {
				var innerReply QueryReply
				ok := srv.Call("ShardMaster.Query", args, &innerReply)
				if ok && innerReply.WrongLeader == false && innerReply.Err != ErrTimeOut{
					ck.lastLeader = i
					reply = innerReply
					break FindLeader
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if reply.Err == OK {
		return reply.Config
	}else{
		return Config{}
	}
}

type argWrapper struct{
	opType  string
	arg  interface{}
}

func (ck *Clerk) modify(args argWrapper){
	ck.seqNum += 1
	var joinArg JoinArgs
	var leaveArg LeaveArgs
	var moveArg MoveArgs
	switch args.opType{
		case "join" :
			joinArg = args.arg.(JoinArgs)
			reply := JoinReply{}
			ok := ck.servers[ck.lastLeader].Call("ShardMaster.Join", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err!=ErrTimeOut {
				return
			}
		case "leave" :
			leaveArg = args.arg.(LeaveArgs)
			reply := LeaveReply{}
			ok := ck.servers[ck.lastLeader].Call("ShardMaster.Leave", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err!=ErrTimeOut {
				return
			}
		case "move" :
			moveArg = args.arg.(MoveArgs)
			reply := MoveReply{}
			ok := ck.servers[ck.lastLeader].Call("ShardMaster.Move", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err!=ErrTimeOut {
				return
			}
		default :
			DPrintf("wrong arg type\n")
	}
	

	for {
		for i := 0; i < len(ck.servers); i++ {
			switch args.opType{
				case "join" :
					reply := JoinReply{}
					ok := ck.servers[i].Call("ShardMaster.Join", joinArg, &reply)
					if ok && !reply.WrongLeader && reply.Err!=ErrTimeOut {
						ck.lastLeader = i
						return
					}
				case "leave" :
					reply := LeaveReply{}
					ok := ck.servers[i].Call("ShardMaster.Leave", leaveArg, &reply)
					if ok && !reply.WrongLeader && reply.Err!=ErrTimeOut {
						ck.lastLeader = i
						return
					}
				case "move" :
					reply := MoveReply{}
					ok := ck.servers[i].Call("ShardMaster.Move", moveArg, &reply)
					if ok && !reply.WrongLeader && reply.Err!=ErrTimeOut {
						ck.lastLeader = i
						return
					}
				default :
					DPrintf("wrong arg type\n")
			}
		}
		time.Sleep(time.Millisecond * 1000)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqNum += 1
	args := JoinArgs{
		Servers : servers,
		ClientId : ck.clientId,
		SeqNum : ck.seqNum,
	}
	// Your code here.
	ck.modify(argWrapper{
		opType : "join",
		arg : args,
		})
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqNum += 1
	args := LeaveArgs{
		GIDs : gids,
		ClientId : ck.clientId,
		SeqNum : ck.seqNum,
	}
	// Your code here.
	ck.modify(argWrapper{
		opType : "leave",
		arg : args,
		})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqNum += 1
	args := MoveArgs{
		Shard : shard,
		GID : gid,
		ClientId : ck.clientId,
		SeqNum : ck.seqNum,
	}
	// Your code here.
	ck.modify(argWrapper{
		opType : "move",
		arg : args,
		})
}
