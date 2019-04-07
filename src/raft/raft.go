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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct{
	Command 	interface{}
	Term 		int
}

const(
	FOLLOWER 	=	"follower"
	LEADER 		=	"leader"
	CANDIDATE 	=	"candidate"

	TIMEOUT 	= 	500
	HEARTBEAT	=	200
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	doneCh			chan interface{}
	applyCh			chan ApplyMsg
	state 			string
	ticketsReceived	int
	timer 			*time.Timer
	
	//Persist
	currentTerm	int
	votedFor	int
	log 		[]LogEntry

	//Volatile
	commitIndex	int
	lastApplied	int

	//Volatile (only on leader)
	nextIndex	[]int
	matchIndex	[]int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	rf.mu.Unlock()
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.setState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetTimer(TIMEOUT)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		may_grant_vote := true
		if len(rf.log) > 0 {
			term := rf.log[len(rf.log)-1].Term
			if args.LastLogTerm < term {
				may_grant_vote = false
			}
			if(args.LastLogTerm == term && args.LastLogIndex + 1 < len(rf.log)){
				may_grant_vote = false
			}
		}

		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = may_grant_vote
		if(may_grant_vote){
			DPrintf("server %d send ticket\n", rf.me)
			rf.resetTimer(TIMEOUT)
		}
		return
	}
}

type AppendEntriesArgs struct{
	Term 			int
	LeaderId 		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct{
	Term 			int
	Success 		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("server %d get append\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%s : server %d hear from leader\n",time.Now() ,rf.me)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.setState(FOLLOWER)
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetTimer(TIMEOUT)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.doneCh)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(int64(me))

	rf.doneCh = make(chan interface{})
	rf.applyCh = applyCh
	rf.setState(FOLLOWER)
	rf.ticketsReceived = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry,0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	rf.resetTimer(TIMEOUT)
	go rf.waitForElection()

	return rf
}

func (rf *Raft) resetTimer(basicTime int) {
	waitTime := rand.Int()%100
	waitTime += basicTime
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * time.Duration(waitTime))
	}else{
		if !rf.timer.Stop(){
			select{
				case <- rf.timer.C:
				default:
			}
		}
		rf.timer.Reset(time.Millisecond * time.Duration(waitTime))
	}
}

func (rf *Raft) sendRequestVotes(args RequestVoteArgs) {
	DPrintf("server %d send request for vote\n", rf.me)
	for i := 0; i < len(rf.peers); i++ {
		if i==rf.me {
			continue
		}
		go func(server int){
			reply := RequestVoteReply{};
			ok := rf.sendRequestVote(server,&args,&reply)
			if(ok){
				rf.handleVoteReply(reply)
			}
		}(i)
	}
}

func (rf *Raft) sendAppendEntries2All(args AppendEntriesArgs) {
	for i := 0; i < len(rf.peers); i++ {
		if i==rf.me {
			continue
		}
		go func(server int){
			reply := AppendEntriesReply{};
			ok := rf.sendAppendEntries(server,&args,&reply)
			if(ok){
				rf.handleAppendReply(reply)
			}
		}(i)
	}
}

func (rf *Raft) handleAppendReply(reply AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.setState(FOLLOWER)
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.resetTimer(TIMEOUT)
	}
}



func (rf *Raft) handleVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
    defer rf.mu.Unlock()

    if reply.Term < rf.currentTerm {
    	return
    }

    if reply.Term > rf.currentTerm {
		rf.setState(FOLLOWER)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer(TIMEOUT)
    }

    if rf.state == CANDIDATE && reply.VoteGranted {
    	rf.ticketsReceived += 1
		DPrintf("server %d get ticket\n", rf.me)
    	if rf.ticketsReceived > len(rf.peers)/2 {
			rf.setState(LEADER)
    		for i := 0; i < len(rf.peers); i++ {
    			if i==rf.me {
    				continue
    			}
    			rf.nextIndex[i] = len(rf.log)
    			rf.matchIndex[i] = -1
    		}
			rf.sendAppendEntries2All(AppendEntriesArgs{
				Term 	:	rf.currentTerm,
				})
    		rf.resetTimer(HEARTBEAT)
    	}
    }
}


func (rf *Raft) waitForElection() {
	for{
		rf.mu.Lock()
		select{
		case <- rf.doneCh:
			return
		case <- rf.timer.C :
			if rf.state != LEADER {
				DPrintf("server %d didn't hear from leader\n", rf.me)
				rf.setState(CANDIDATE)
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.ticketsReceived = 1
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
	            	CandidateId:  rf.me,
	            	LastLogIndex: len(rf.log) - 1,
				};
				if len(rf.log) > 0 {
		            args.LastLogTerm = rf.log[args.LastLogIndex].Term
		        }
				rf.sendRequestVotes(args)
				rf.resetTimer(TIMEOUT)
			}else{
				rf.sendAppendEntries2All(AppendEntriesArgs{
					Term 	:	rf.currentTerm,
					})
				rf.resetTimer(HEARTBEAT)
			}
		default:
		}
		fmt.Printf("")
		rf.mu.Unlock()
	}
}

func (rf *Raft) setState(state string) {
	DPrintf("server %d becomes %s\n",rf.me,state)
	rf.state = state
}
