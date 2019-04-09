package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
	"strconv"
)

const (
	Debug 	= 0

	GET		= 0
	PUT 	= 1
	APPEND 	= 2

	TIMEOUT = 100
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

/*func (kv *KVServer) resetTimer() {
	if kv.timer == nil {
		kv.timer = time.NewTimer(time.Millisecond * time.Duration(TIMEOUT))
	}else{
		if !kv.timer.Stop(){
			select{
				case <- kv.timer.C:
				default:
			}
		}
		kv.timer.Reset(time.Millisecond * time.Duration(TIMEOUT))
	}
}*/


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType	int
	Key		string
	Value 	string
	Id 		string
	ClientId int
	SeqNum 	int
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	done map[string]chan int
	result map[string]string
	maxSeqNum map[int]int
	doneCh chan int
	recoverLogNum int
}

func rpcRequestId(client int, seqNum int) string {
	return strconv.Itoa(client) + "," + strconv.Itoa(seqNum)
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	opId := rpcRequestId(args.ClientId,args.SeqNum)

	op := Op{
		OpType : GET,
		Key : args.Key,
		Id : opId,
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false

	doneCh := make(chan int)
	_,ok := kv.done[opId]
	if ok {
		DPrintf("-----------------------------------------\n")
		DPrintf("kvserver %d: channel for op %d exist\n",kv.me,opId)
	}
	kv.done[opId] = doneCh

	kv.mu.Unlock()
	done := false
	Done:
	for i := 0; i < 5; i++ {
		select{
		case <- doneCh:
			done = true
			break Done
		default:
		}
		time.Sleep(time.Millisecond * TIMEOUT)
	}
	kv.mu.Lock()

	if kv.result[opId] == "" {
		reply.Err = ErrNoKey
	}else{
		reply.Err = OK
	}
	if !done {
		reply.Err = ErrTimeOut
	}
	reply.Value = kv.result[opId]

	delete(kv.result, opId)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	opId := rpcRequestId(args.ClientId,args.SeqNum)

	op := Op{
		Key : args.Key,
		Value : args.Value,
		Id : opId,
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
	}
	switch args.Op{
	case "Put":
		op.OpType = PUT
	case "Append":
		op.OpType = APPEND
	default:
		DPrintf("-----------------------------------------\n")
		DPrintf("kv server %d get wrong op type %s\n",kv.me,args.Op)
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false

	doneCh := make(chan int)
	_,ok := kv.done[opId]
	if ok {
		DPrintf("-----------------------------------------\n")
		DPrintf("kvserver %d: channel for op %d exist\n",kv.me,opId)
	}
	kv.done[opId] = doneCh

	kv.mu.Unlock()
	done := false
	Done:
	for i := 0; i < 5; i++ {
		select{
		case <- doneCh:
			done = true
			break Done
		default:
		}
		time.Sleep(time.Millisecond * TIMEOUT)
	}
	kv.mu.Lock()

	reply.Err = OK
	if !done {
		reply.Err = ErrTimeOut
	}
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	close(kv.doneCh)
	kv.mu.Unlock()
}

//
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
//
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
	kv.data = make(map[string]string)
	kv.done = make(map[string]chan int)
	kv.result = make(map[string]string)
	kv.maxSeqNum = make(map[int]int)
	kv.doneCh = make(chan int)
	kv.recoverLogNum = 0
	go kv.applyOps()
	//go kv.aliveCheck()

	fmt.Printf("")
	return kv
}

func (kv *KVServer) aliveCheck() {
	for{
		kv.mu.Lock()
		select{
		case <- kv.doneCh:
			return
		default:
		}
		fmt.Printf("kv server %d alive\n",kv.me)
		kv.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (kv *KVServer) applyOps(){
	for msg := range kv.applyCh{
		kv.applyOp(msg)
	}
}

func (kv *KVServer) applyOp(msg raft.ApplyMsg){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op,ok := msg.Command.(Op)
	if !ok {
		DPrintf("-----------------------------------------\n")
		DPrintf("kv server %d get wrong msg command type %T\n",kv.me,msg.Command)
		return
	}

	maxSeqNum , ok := kv.maxSeqNum[op.ClientId]
	if !ok {
		kv.maxSeqNum[op.ClientId] = 0
		maxSeqNum = 0
	}

	duplicated := true
	switch {
	case maxSeqNum == op.SeqNum:
		duplicated = true
	case maxSeqNum == op.SeqNum - 1:
		duplicated = false
		kv.maxSeqNum[op.ClientId] = op.SeqNum
	default:
		DPrintf("-----------------------------------------\n")
		DPrintf("client %d has applied %d but get %d afterwards\n",op.ClientId,op.SeqNum,maxSeqNum)
	}

	_, hasChannel := kv.done[op.Id]

	switch op.OpType{
	case GET:
		value, ok := kv.data[op.Key]
		if hasChannel {
			if ok {
				kv.result[op.Id] = value
			}else{
				kv.result[op.Id] = ""
			}
			DPrintf("client %d opSeq %d get : key=%s result=%s\n",op.ClientId,op.SeqNum,op.Key,value)
		}
	case PUT:
		if !duplicated {
			kv.data[op.Key] = op.Value
		}
		if hasChannel {
			DPrintf("client %d opSeq %d put : key=%s value=%s\n",op.ClientId,op.SeqNum,op.Key,op.Value)
		}
	case APPEND:
		if !duplicated {
			value, ok := kv.data[op.Key]
			if ok {
				kv.data[op.Key] = value + op.Value
			}else{
				kv.data[op.Key] = op.Value
			}
		}
		if hasChannel {
			DPrintf("client %d opSeq %d append : key=%s value=%s result=%s\n",op.ClientId,op.SeqNum,op.Key,op.Value,kv.data[op.Key])
		}
	default:
		DPrintf("-----------------------------------------\n")
		DPrintf("kv server %d get wrong op type %d\n",kv.me,op.OpType)
	}
	if hasChannel {
		close(kv.done[op.Id])
		delete(kv.done, op.Id)
	}
}