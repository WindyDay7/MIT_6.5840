package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	StateFollower  = 0
	StateCandidate = 1
	StateLeader    = 2
)

// A LogEntry is a single entry in the Raft log. It contains a command,
// the term when the entry was received by the leader, and the index of the entry in the log.
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers:
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// for leader
	// nextIndex[i] is the index of the next log entry to send to that server (initialized to leader last log index + 1)
	// matchIndex[i] is the index of highest log entry known to be replicated on server i
	nextIndex  []int
	matchIndex []int

	// state of the server
	state int
	// record the time when the last heartbeat was received, used for election timeout
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
	applyCh chan raftapi.ApplyMsg
}

// get the index of the last log entry, or 0 if the log is empty
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

// get the term of the last log entry, or 0 if the log is empty
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // last command's term in candidate's log
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	candicateTerm := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if candidate's term is less or equal to current term, reject vote
	if candicateTerm <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's term is greater than current term, update current term and reset votedFor
	if candicateTerm > rf.currentTerm {
		rf.currentTerm = candicateTerm
		rf.votedFor = -1
		rf.state = StateFollower
	}

	// if we haven't voted for anyone or have voted for the candidate, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == candidateId) && (lastLogTerm > rf.getLastLogTerm() || (lastLogTerm == rf.getLastLogTerm() && lastLogIndex >= rf.getLastLogIndex())) {
		rf.votedFor = candidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	leaderTerm := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	new_log_entries := args.Entries
	leaderCommit := args.LeaderCommit

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if leader's term is less than current term, reject append
	if leaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if leader's term is greater than current term, update current term and reset votedFor
	if leaderTerm > rf.currentTerm {
		rf.currentTerm = leaderTerm
		rf.votedFor = -1
		rf.state = StateFollower
	}

	rf.lastHeartbeat = time.Now()
	rf.state = StateFollower // we recognize them as a legitimate leader

	// if log doesn't contain an entry at prevLogIndex whose term also matches prevLogTerm, otherwise reject append
	if prevLogIndex > rf.getLastLogIndex() || rf.log[prevLogIndex].Term != prevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	for i, entry := range new_log_entries {
		index := prevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term {
				// delete the existing entry and all that follow it
				rf.log = rf.log[:index]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, rf.getLastLogIndex())
	}

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should pass &reply.
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if this server isn't the leader, return false
	isLeader = rf.state == StateLeader
	if !isLeader {
		return index, term, isLeader
	}
	term = rf.currentTerm

	// append the new command to the log
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
		Index:   len(rf.log),
	})
	index = len(rf.log) - 1

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
	// get the current value of rf.dead atomically.
	// 0 means "not killed". 1 means "killed".
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == StateLeader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				// Prepare args inside the lock
				prevLogIndex := rf.nextIndex[i] - 1
				// Note: log is 0-indexed effectively. If prevLogIndex is 0, we can access rf.log[0]
				prevLogTerm := rf.log[prevLogIndex].Term

				// Make a copy of entries to send. (rf.log[rf.nextIndex[i]:] contains all entries from nextIndex)
				// if rf.nextIndex[i] is greater than the last log index, rf.log[rf.nextIndex[i]:] will be an empty slice,
				// which is fine for heartbeats.
				entries := make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
				copy(entries, rf.log[rf.nextIndex[i]:])

				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock() // Unlock before sending RPC

				// Launch a separate goroutine for each peer
				go func(server int, args *AppendEntriesArgs) {
					reply := &AppendEntriesReply{}

					// 1. Call RPC without holding any locks!
					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
						return
					}

					// 2. Lock again to process the response
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 3. Very important: check if we are still the leader in the same term
					if rf.currentTerm != args.Term || rf.state != StateLeader {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = StateFollower
						rf.votedFor = -1
						return
					}

					// 4. Process the response and update nextIndex/matchIndex
					if reply.Success {
						rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[server] = rf.nextIndex[server] - 1

						// If majority matchIndex >= N, and log[N].term == currentTerm, update commitIndex
						// N is the last log index we can safely commit, which is the minimum of matchIndex of majority and the last log index
						for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
							matchCount := 1 // count self
							for j := 0; j < len(rf.peers); j++ {
								if j != rf.me && rf.matchIndex[j] >= N {
									matchCount++
								}
							}
							if matchCount > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
								rf.commitIndex = N
								break
							}
						}
					} else {
						// Simple decrement for Lab 3B (can be optimized later)
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						}
					}
				}(i, args)

				rf.mu.Lock() // Re-lock before the next iteration
			}
			rf.mu.Unlock() // Unlock before sleeping!
			time.Sleep(120 * time.Millisecond)
			continue
		}
		// sleep for a while to avoid busy loop when we're not the leader
		if time.Since(rf.lastHeartbeat) < rf.electionTimeout {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// if we haven't received a heartbeat from the leader for electionTimeout, start an election
		rf.state = StateCandidate
		rf.currentTerm += 1
		rf.votedFor = rf.me // vote for ourselves
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = time.Duration(300+(rand.Int63()%200)) * time.Millisecond

		candidateTerm := rf.currentTerm
		candidateID := rf.me
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		// vote for ourselves
		votes := 1
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			args := &RequestVoteArgs{
				Term:         candidateTerm,
				CandidateId:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			go func(server int, requestArgs *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, requestArgs, reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// if we receive a RequestVote reply with a term greater than our current term, we update our term and step down to follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = StateFollower
					return
				}
				// avoid counting votes from stale RequestVote RPCs that arrive after we've already become a leader or stepped down to follower
				if rf.state != StateCandidate || rf.currentTerm != requestArgs.Term {
					return
				}

				if !reply.VoteGranted {
					return
				}

				votes += 1
				if votes <= len(rf.peers)/2 {
					return
				}

				rf.state = StateLeader
				// initialize nextIndex and matchIndex for each follower
				for j := 0; j < len(rf.peers); j++ {
					rf.nextIndex[j] = rf.getLastLogIndex() + 1
					rf.matchIndex[j] = 0
				}

				// immediately send heartbeats to establish authority
				leaderTerm := rf.currentTerm
				leaderID := rf.me
				prevLogIndex := rf.getLastLogIndex()
				prevLogTerm := rf.getLastLogTerm()
				leaderCommit := rf.commitIndex

				for j := 0; j < len(rf.peers); j++ {
					if j == rf.me {
						continue
					}
					hbArgs := &AppendEntriesArgs{
						Term:         leaderTerm,
						LeaderId:     leaderID,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      []LogEntry{},
						LeaderCommit: leaderCommit,
					}
					hbReply := &AppendEntriesReply{}
					go rf.sendAppendEntries(j, hbArgs, hbReply)
				}
			}(i, args)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// applyEntries should be called periodically or after commitIndex changes
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			// Copy entries to apply to avoid holding lock while sending on applyCh
			firstUnapplied := rf.lastApplied + 1
			lastCommit := rf.commitIndex
			entries := make([]LogEntry, lastCommit-firstUnapplied+1)
			copy(entries, rf.log[firstUnapplied:lastCommit+1])
			rf.mu.Unlock()

			for _, entry := range entries {
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.applyCh <- applyMsg

				rf.mu.Lock()
				if entry.Index > rf.lastApplied {
					rf.lastApplied = entry.Index
				}
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = StateFollower
	rf.lastHeartbeat = time.Now()

	// if the log is empty, put a dummy log entry at index 0 to make the log start from index 1,
	rf.log = append(rf.log, LogEntry{
		Command: "",
		Term:    rf.currentTerm,
		Index:   0,
	})

	// randomize election timeout to avoid split votes
	rf.electionTimeout = time.Duration(300+(rand.Int63()%200)) * time.Millisecond

	// for leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to periodically apply committed entries
	go rf.applier()

	return rf
}
