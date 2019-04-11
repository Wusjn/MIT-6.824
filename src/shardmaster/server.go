package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "strconv"
import "bytes"
import "log"

const (
	Debug 	= 1

	JOIN	= 0
	LEAVE 	= 1
	MOVE 	= 2
	QUERY 	= 3

	TIMEOUT = 500
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType	int
	Servers map[int][]string
	GIDs []int
	Shard int
	GID   int
	Num int

	Id 		string
	ClientId int
	SeqNum 	int
}


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxraftstate int

	persister *raft.Persister
	configs []Config // indexed by config num
	done map[string]chan int
	result map[string]Config
	maxSeqNum map[int]int
	doneCh chan int
}

func rpcRequestId(client int, seqNum int) string {
	return strconv.Itoa(client) + "," + strconv.Itoa(seqNum)
}

type ModifyReply struct{
	WrongLeader bool
	Err         Err
}

func (sm *ShardMaster) modify(op *Op) (reply ModifyReply) {
	sm.mu.Lock()

	reply = ModifyReply{}

	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	reply.WrongLeader = false

	maxSeqNum , ok := sm.maxSeqNum[op.ClientId]
	if !ok {
		sm.maxSeqNum[op.ClientId] = 0
		maxSeqNum = 0
	}
	if maxSeqNum >= op.SeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	}


	doneCh := make(chan int)
	sm.done[op.Id] = doneCh

	sm.mu.Unlock()
	done := false
	select{
	case <- doneCh:
		done = true
	case <-time.After(time.Millisecond * TIMEOUT):
	}
	sm.mu.Lock()

	reply.Err = OK
	if !done {
		reply.Err = ErrTimeOut
	}
	sm.mu.Unlock()
	return
} 

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType 	: JOIN,
		Servers : args.Servers,
		Id : rpcRequestId(args.ClientId,args.SeqNum),
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
	}

	modifyReply := sm.modify(&op)

	reply.Err = modifyReply.Err
	reply.WrongLeader = modifyReply.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType 	: LEAVE,
		GIDs : args.GIDs,
		Id : rpcRequestId(args.ClientId,args.SeqNum),
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
	}

	modifyReply := sm.modify(&op)

	reply.Err = modifyReply.Err
	reply.WrongLeader = modifyReply.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType 	: JOIN,
		Shard : args.Shard,
		GID : args.GID,
		Id : rpcRequestId(args.ClientId,args.SeqNum),
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
	}

	modifyReply := sm.modify(&op)

	reply.Err = modifyReply.Err
	reply.WrongLeader = modifyReply.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()

	opId := rpcRequestId(args.ClientId,args.SeqNum)

	op := Op{
		OpType : QUERY,
		Num : args.Num,
		Id : opId,
		ClientId : args.ClientId,
		SeqNum : args.SeqNum,
	}
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	reply.WrongLeader = false

	maxSeqNum , ok := sm.maxSeqNum[op.ClientId]
	if !ok {
		sm.maxSeqNum[op.ClientId] = 0
		maxSeqNum = 0
	}
	if maxSeqNum >= op.SeqNum {
		if op.Num==-1 || op.Num>=len(sm.configs) {
			reply.Err = OK
			reply.Config = sm.configs[len(sm.configs)-1]
		}else{
			if op.Num < -1 {
				reply.Err = ErrNoKey
			}else{
				reply.Config = sm.configs[op.Num]
				reply.Err = OK
			}
		}
		sm.mu.Unlock()
		return
	}

	doneCh := make(chan int)
	sm.done[opId] = doneCh

	sm.mu.Unlock()
	done := false
	select{
	case <- doneCh:
		done = true
	case <-time.After(time.Millisecond * TIMEOUT):
	}
	sm.mu.Lock()

	if _,ok := sm.result[opId]; !ok {
		reply.Err = ErrNoKey
	}else{
		reply.Err = OK
	}
	if !done {
		reply.Err = ErrTimeOut
	}
	reply.Config = sm.result[opId]

	delete(sm.result, opId)
	sm.mu.Unlock()
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	close(sm.doneCh)
	sm.mu.Unlock()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me
	sm.maxraftstate = -1

	sm.applyCh = make(chan raft.ApplyMsg)

	sm.configs = make([]Config, 1)

	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sm.configs[0].Shards[i] = 0
	}

	// Your code here.
	sm.persister = persister
	sm.done = make(map[string]chan int)
	sm.result = make(map[string]Config)
	sm.maxSeqNum = make(map[int]int)
	sm.doneCh = make(chan int)

	sm.readSnapshot(persister.ReadSnapshot())
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.applyOps()

	return sm
}

func (sm *ShardMaster) applyOps(){
	for msg := range sm.applyCh{
		sm.applyOp(msg)
	}
}

func copyConfig(origin Config) Config{
	newConfig := Config{
		Num : origin.Num,
		Shards : origin.Shards,
	}
	newConfig.Groups = make(map[int][]string)
	for k,v := range origin.Groups{
		newConfig.Groups[k] = v
	}
	return newConfig
}

func redistribute(config *Config){
	GIDs := make([]int,0)
	for k,_ := range config.Groups{
		GIDs = append(GIDs,k)
	}
	//TODO
}

func opJoin(config *Config, servers map[int][]string) {
	config.Num += 1
	for k,v := range servers{
		config.Groups[k] = v
	}
	redistribute(config)
}

func opLeave(config *Config, GIDs []int) {
	config.Num += 1
	for _,GID := range GIDs{
		delete(config.Groups,GID)
	}
	redistribute(config)
}

func opMove(config *Config, shard int, GID int) {
	config.Num += 1
	config.Shards[shard] = GID
}

func (sm *ShardMaster) applyOp(msg raft.ApplyMsg){
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !msg.CommandValid {
		//snapshot
		cmd,ok := msg.Command.([]byte)
		if !ok {
			log.Printf("sm server %d get wrong msg command type %T\n",sm.me,msg.Command)
			//log.Fatal("program exist\n")
		}else{
			sm.readSnapshot(cmd)
		}
		return
	}

	op,ok := msg.Command.(Op)
	if !ok {
		log.Printf("sm server %d get wrong msg command type %T\n",sm.me,msg.Command)
		//log.Fatal("program exist\n")
		return
	}

	maxSeqNum , ok := sm.maxSeqNum[op.ClientId]
	if !ok {
		sm.maxSeqNum[op.ClientId] = 0
		maxSeqNum = 0
	}

	duplicated := true
	switch {
	case maxSeqNum == op.SeqNum:
		duplicated = true
	case maxSeqNum == op.SeqNum - 1:
		duplicated = false
		sm.maxSeqNum[op.ClientId] = op.SeqNum
	default:
		log.Printf("client %d has applied %d but get %d afterwards\n",op.ClientId,maxSeqNum,op.SeqNum)
		log.Fatal("program exist\n")
	}

	_, hasChannel := sm.done[op.Id]

	switch op.OpType{
	case QUERY:
		var config Config
		validConfig := true
		if op.Num==-1 || op.Num>=len(sm.configs) {
			config = sm.configs[len(sm.configs)-1]
		}else{
			if op.Num < -1 {
				validConfig = false
			}else{
				config = sm.configs[op.Num]
			}
		}
		if hasChannel {
			if !validConfig {
				delete(sm.result,op.Id)
			}else{
				sm.result[op.Id] = copyConfig(config)
			}
		}
	case JOIN:
		if !duplicated {
			config := copyConfig(sm.configs[len(sm.configs)-1])
			opJoin(&config,op.Servers)
			sm.configs = append(sm.configs,config)
		}
	case LEAVE:
		if !duplicated {
			config := copyConfig(sm.configs[len(sm.configs)-1])
			opLeave(&config,op.GIDs)
			sm.configs = append(sm.configs,config)
		}
	case MOVE:
		if !duplicated {
			config := copyConfig(sm.configs[len(sm.configs)-1])
			opMove(&config,op.Shard,op.GID)
			sm.configs = append(sm.configs,config)
		}
	default:
		log.Printf("sm server %d get wrong op type %d\n",sm.me,op.OpType)
		//log.Fatal("program exist\n")
	}
	if hasChannel {
		close(sm.done[op.Id])
		delete(sm.done, op.Id)
	}
	sm.snapshotIfNeeded(msg.CommandIndex)
}

func (sm *ShardMaster) snapshotIfNeeded(lastCommandIdx int){
	if sm.maxraftstate!=-1 &&
		sm.persister.RaftStateSize() > sm.maxraftstate &&
		lastCommandIdx - sm.rf.GetBasicIdx() > 10 {
		sm.snapshot(lastCommandIdx)
	}
}

func (sm *ShardMaster) snapshot(lastCommandIdx int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.maxSeqNum)
	snapshot := w.Bytes()
	sm.rf.PersistAndSaveSnapshot(lastCommandIdx,snapshot)
}

func (sm *ShardMaster) readSnapshot(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	d := labgob.NewDecoder(bytes.NewBuffer(snapshot))
	if d.Decode(&sm.configs) != nil ||
		d.Decode(&sm.maxSeqNum) != nil {
		log.Fatal("Error in reading snapshot")
	}
}
