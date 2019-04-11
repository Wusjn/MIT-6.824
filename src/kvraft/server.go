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
	"bytes"
)

const (
	Debug 	= 1

	GET		= 0
	PUT 	= 1
	APPEND 	= 2

	TIMEOUT = 500
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
	persister *raft.Persister

	// Your definitions here.
	data map[string]string
	done map[string]chan int
	result map[string]string
	maxSeqNum map[int]int
	doneCh chan int
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

	maxSeqNum , ok := kv.maxSeqNum[op.ClientId]
	if !ok {
		kv.maxSeqNum[op.ClientId] = 0
		maxSeqNum = 0
	}
	if maxSeqNum >= op.SeqNum {
		value, ok := kv.data[op.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		}else{
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		kv.mu.Unlock()
		return
	}

	doneCh := make(chan int)
	kv.done[opId] = doneCh

	kv.mu.Unlock()
	done := false
	select{
	case <- doneCh:
		done = true
	case <-time.After(time.Millisecond * TIMEOUT):
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
		log.Printf("kv server %d get wrong op type %s\n",kv.me,args.Op)
		//log.Fatal("program exist\n")
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = false

	maxSeqNum , ok := kv.maxSeqNum[op.ClientId]
	if !ok {
		kv.maxSeqNum[op.ClientId] = 0
		maxSeqNum = 0
	}
	if maxSeqNum >= op.SeqNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}


	doneCh := make(chan int)
	kv.done[opId] = doneCh

	kv.mu.Unlock()
	done := false
	select{
	case <- doneCh:
		done = true
	case <-time.After(time.Millisecond * TIMEOUT):
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

	kv.applyCh = make(chan raft.ApplyMsg,10)

	// You may need initialization code here.
	kv.persister = persister
	kv.data = make(map[string]string)
	kv.done = make(map[string]chan int)
	kv.result = make(map[string]string)
	kv.maxSeqNum = make(map[int]int)
	kv.doneCh = make(chan int)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

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

	if !msg.CommandValid {
		//snapshot
		cmd,ok := msg.Command.([]byte)
		if !ok {
			log.Printf("kv server %d get wrong msg command type %T\n",kv.me,msg.Command)
			//log.Fatal("program exist\n")
		}else{
			kv.readSnapshot(cmd)
		}
		return
	}

	op,ok := msg.Command.(Op)
	if !ok {
		log.Printf("kv server %d get wrong msg command type %T\n",kv.me,msg.Command)
		//log.Fatal("program exist\n")
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
		log.Printf("client %d has applied %d but get %d afterwards\n",op.ClientId,maxSeqNum,op.SeqNum)
		log.Fatal("program exist\n")
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
			//DPrintf("server %d: client %d opSeq %d get : key=%s result=%s\n",kv.me,op.ClientId,op.SeqNum,op.Key,value)
		}
	case PUT:
		if !duplicated {
			kv.data[op.Key] = op.Value
		}
		if hasChannel {
			//DPrintf("server %d: client %d opSeq %d put : key=%s value=%s\n",kv.me,op.ClientId,op.SeqNum,op.Key,op.Value)
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
			//DPrintf("server %d: client %d opSeq %d append : key=%s value=%s result=%s\n",kv.me,op.ClientId,op.SeqNum,op.Key,op.Value,kv.data[op.Key])
		}
	default:
		log.Printf("kv server %d get wrong op type %d\n",kv.me,op.OpType)
		//log.Fatal("program exist\n")
	}
	if hasChannel {
		close(kv.done[op.Id])
		delete(kv.done, op.Id)
	}
	kv.snapshotIfNeeded(msg.CommandIndex)
}

func (kv *KVServer) snapshotIfNeeded(lastCommandIdx int){
	if kv.maxraftstate!=-1 &&
		kv.persister.RaftStateSize() > kv.maxraftstate &&
		lastCommandIdx - kv.rf.GetBasicIdx() > 10 {
		kv.snapshot(lastCommandIdx)
	}
}

func (kv *KVServer) snapshot(lastCommandIdx int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.maxSeqNum)
	snapshot := w.Bytes()
	kv.rf.PersistAndSaveSnapshot(lastCommandIdx,snapshot)
}

func (kv *KVServer) readSnapshot(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	d := labgob.NewDecoder(bytes.NewBuffer(snapshot))
	if d.Decode(&kv.data) != nil ||
		d.Decode(&kv.maxSeqNum) != nil {
		log.Fatal("Error in reading snapshot")
	}
}