package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

type SnapshotpersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	Peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *tester.Persister   // Object to hold this peer's persisted state
	Me        int                 // this peer's index into peers[]
	Dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// volatile state on all servers:
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	CommitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	LastApplied int

	// for leader
	// nextIndex[i] is the index of the next log entry to send to that server (initialized to leader last log index + 1)
	// matchIndex[i] is the index of highest log entry known to be replicated on server i
	NextIndex  []int
	MatchIndex []int

	// state of the server
	State int
	// record the time when the last heartbeat was received, used for election timeout
	ElectionTimeout time.Duration
	LastHeartbeat   time.Time

	// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
	ApplyCh chan raftapi.ApplyMsg

	// Snapshot related
	// last included index and term in the snapshot
	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot_Byte      []byte
	Temp_Snapshot_Byte []byte
	// SnapshotPending indicates whether there is a snapshot that needs to be applied to the state machine
	SnapshotPending bool
}

// get the index of the last log entry, or 0 if the log is empty
func (rf *Raft) getLastLogIndex() int {
	if len(rf.Log) == 0 {
		return rf.LastIncludedIndex
	}
	return rf.Log[len(rf.Log)-1].Index
}

// get the term of the last log entry, or 0 if the log is empty
func (rf *Raft) getLastLogTerm() int {
	if len(rf.Log) == 0 {
		return rf.LastIncludedTerm
	}
	return rf.Log[len(rf.Log)-1].Term
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.Persister.Save(raftstate, rf.Snapshot_Byte)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		// error...
		return
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm

		if rf.LastIncludedIndex > 0 {
			rf.CommitIndex = rf.LastIncludedIndex
			rf.LastApplied = rf.LastIncludedIndex
		}
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.Persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	if index <= rf.LastIncludedIndex {
		return
	}

	adjustedIndex := index - rf.LastIncludedIndex

	// update snapshot related fields
	rf.LastIncludedTerm = rf.Log[adjustedIndex].Term

	newLog := make([]LogEntry, len(rf.Log[adjustedIndex:]))
	copy(newLog, rf.Log[adjustedIndex:])
	rf.Log = newLog

	rf.LastIncludedIndex = index
	rf.Snapshot_Byte = snapshot
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	reply.Term = rf.CurrentTerm

	// 1. Check Term: if the leader's term is less than the current term, reject the snapshot
	if args.Term < rf.CurrentTerm {
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	rf.State = StateFollower

	// reset Heaertbeat time to avoid unnecessary elections
	rf.LastHeartbeat = time.Now()

	// 2. deal with the snapshot data
	if args.Offset == 0 {
		rf.Temp_Snapshot_Byte = make([]byte, 0)
	}

	// append the snapshot data normally
	rf.Temp_Snapshot_Byte = append(rf.Temp_Snapshot_Byte, args.Data...)

	if !args.Done {
		return
	}

	// 3. Check if the snapshot is old or outdated
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	}

	// 4. Update the in-memory snapshot data
	rf.Snapshot_Byte = rf.Temp_Snapshot_Byte
	rf.Temp_Snapshot_Byte = nil

	// 5. Safely truncate the log (resolve Panic issue)
	// Find the actual physical position of LastIncludedIndex in the current rf.Log
	hasLogMatch := false

	// If the log is not empty, check if the LastIncludedIndex falls within the range of the current in-memory log
	if len(rf.Log) > 0 {
		firstLogIndex := rf.Log[0].Index
		lastLogIndex := rf.getLastLogIndex()

		// Check if the LastIncludedIndex of the snapshot falls within the range of the current in-memory log
		if args.LastIncludedIndex >= firstLogIndex && args.LastIncludedIndex <= lastLogIndex {
			// find the physical position of LastIncludedIndex in the current rf.Log
			targetIndex := args.LastIncludedIndex - firstLogIndex

			// check if the term matches
			if rf.Log[targetIndex].Term == args.LastIncludedTerm {
				hasLogMatch = true

				// Truncate the log to remove entries up to LastIncludedIndex
				tempLog := make([]LogEntry, 0)
				tempLog = append(tempLog, rf.Log[targetIndex:]...)
				rf.Log = tempLog
			}
		}
	}

	if !hasLogMatch {
		// If the log is empty or the LastIncludedIndex does not match any entry in the current log,
		// we can safely reset the log to an empty state, but we must keep a dummy entry
		rf.Log = make([]LogEntry, 1)
		rf.Log[0] = LogEntry{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
		}
	}

	// Update snapshot metadata
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm

	// Update CommitIndex and LastApplied
	if rf.CommitIndex < rf.LastIncludedIndex {
		rf.CommitIndex = rf.LastIncludedIndex
	}
	if rf.LastApplied < rf.LastIncludedIndex {
		// Set SnapshotPending to true to indicate that a snapshot needs to be applied to the state machine
		rf.SnapshotPending = true
	}

	// 6. Save the state and snapshot together (you need to modify the underlying persist logic, do not pass nil)
	rf.persist()
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

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// if candidate's term is less or equal to current term, reject vote
	if candicateTerm <= rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's term is greater than current term, update current term and reset votedFor
	if candicateTerm > rf.CurrentTerm {
		rf.CurrentTerm = candicateTerm
		rf.VotedFor = -1
		rf.State = StateFollower
		rf.persist()
	}

	// if we haven't voted for anyone or have voted for the candidate, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if (rf.VotedFor == -1 || rf.VotedFor == candidateId) && (lastLogTerm > rf.getLastLogTerm() || (lastLogTerm == rf.getLastLogTerm() && lastLogIndex >= rf.getLastLogIndex())) {
		rf.VotedFor = candidateId
		reply.VoteGranted = true
		rf.LastHeartbeat = time.Now()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.CurrentTerm
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
	Term          int
	LogLastIndex  int // for optimization in Lab 3B, the last index of the follower's log
	Xterm         int // for optimization in Lab 3B, the term of the follower's log entry at prevLogIndex
	LogFirstIndex int // for optimization in Lab 3B, the beginning index of the follower's log entry in Xterm
	Success       bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	leaderTerm := args.Term
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	new_log_entries := args.Entries
	leaderCommit := args.LeaderCommit

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// if leader's term is less than current term, reject append
	if leaderTerm < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// if leader's term is greater than current term, update current term and reset votedFor
	if leaderTerm > rf.CurrentTerm {
		rf.CurrentTerm = leaderTerm
		rf.VotedFor = -1
		rf.State = StateFollower
		rf.persist()
	}

	rf.LastHeartbeat = time.Now()
	rf.State = StateFollower // we recognize them as a legitimate leader

	// if prevLogIndex is less than LastIncludedIndex, it means the leader's log is too old, and we should reject the append
	if prevLogIndex < rf.LastIncludedIndex {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.LogLastIndex = rf.getLastLogIndex() + 1
		reply.Xterm = -1
		reply.LogFirstIndex = rf.LastIncludedIndex + 1
		return
	}

	// if log doesn't contain an entry at prevLogIndex whose term also matches prevLogTerm, reject append
	if prevLogIndex > rf.getLastLogIndex() || rf.Log[prevLogIndex-rf.LastIncludedIndex].Term != prevLogTerm {
		reply.Term = rf.CurrentTerm
		reply.LogLastIndex = rf.getLastLogIndex() + 1
		if prevLogIndex > rf.getLastLogIndex() {
			// the entries are too far back
			reply.Xterm = -1
			reply.LogFirstIndex = -1
		} else {
			// Xterm is the term of the conflicting entry, LogFirstIndex is the first index of that term in the log
			reply.Xterm = rf.Log[prevLogIndex-rf.LastIncludedIndex].Term
			// find the first index of the conflicting term
			reply.LogFirstIndex = prevLogIndex
			for reply.LogFirstIndex > rf.LastIncludedIndex && rf.Log[reply.LogFirstIndex-1-rf.LastIncludedIndex].Term == reply.Xterm {
				reply.LogFirstIndex -= 1
			}
		}
		reply.Success = false
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	for i, entry := range new_log_entries {
		index := prevLogIndex + 1 + i - rf.LastIncludedIndex // adjust for the fact that rf.Log is 0-indexed and may have been truncated due to snapshotting
		if index < len(rf.Log) {
			if rf.Log[index].Term != entry.Term {
				// delete the existing entry and all that follow it
				rf.Log = rf.Log[:index]
				rf.Log = append(rf.Log, entry)
			}
		} else {
			rf.Log = append(rf.Log, entry)
		}
	}
	rf.persist()

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(leaderCommit, rf.getLastLogIndex())
	}

	rf.LastHeartbeat = time.Now()
	reply.Term = rf.CurrentTerm
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.Peers[].
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
func (rf *Raft) sendRequestVote() {
	rf.Mu.Lock()
	rf.State = StateCandidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.Me // vote for ourselves
	rf.LastHeartbeat = time.Now()
	rf.ElectionTimeout = time.Duration(300+(rand.Int63()%200)) * time.Millisecond
	rf.persist()

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.Me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.Mu.Unlock()

	var votes int32 = 1

	for i := 0; i < len(rf.Peers); i++ {
		if i == rf.Me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				return
			}

			rf.Mu.Lock()
			defer rf.Mu.Unlock()
			// check if current term and state are still the same, if not, ignore the reply
			if rf.CurrentTerm != args.Term || rf.State != StateCandidate {
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.State = StateFollower
				rf.persist()
				return
			}

			if reply.VoteGranted {
				v := atomic.AddInt32(&votes, 1)
				if int(v) > len(rf.Peers)/2 && rf.State == StateCandidate {
					rf.State = StateLeader
					for j := 0; j < len(rf.Peers); j++ {
						rf.NextIndex[j] = rf.getLastLogIndex() + 1
						rf.MatchIndex[j] = 0
					}
					// send heartbeats immediately
					for j := 0; j < len(rf.Peers); j++ {
						if j == rf.Me {
							continue
						}
						go rf.sendAppendEntries(j)
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) sendSnapshotToFollower(server int) {
	rf.Mu.Lock()
	if rf.State != StateLeader {
		rf.Mu.Unlock()
		return
	}
	Leader_Id := rf.Me
	Leader_Term := rf.CurrentTerm
	Leader_LastIncludedIndex := rf.LastIncludedIndex
	Leader_LastIncludedTerm := rf.LastIncludedTerm
	// deepcopy the snapshot
	var Leader_Snapshot_Byte []byte
	if len(rf.Snapshot_Byte) > 0 {
		Leader_Snapshot_Byte = make([]byte, len(rf.Snapshot_Byte))
		copy(Leader_Snapshot_Byte, rf.Snapshot_Byte)
	}

	args := &InstallSnapshotArgs{
		Term:              Leader_Term,
		LeaderId:          Leader_Id,
		LastIncludedIndex: Leader_LastIncludedIndex,
		LastIncludedTerm:  Leader_LastIncludedTerm,
		Offset:            0,
		Data:              Leader_Snapshot_Byte,
		Done:              true,
	}
	rf.Mu.Unlock() // Unlock before sending RPC

	// send the InstallSnapshot RPC to the follower
	reply := &InstallSnapshotReply{}
	ok := rf.Peers[server].Call("Raft.InstallSnapshot", args, reply)

	if !ok {
		return
	}

	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	// check if we are still the leader and in the same term before processing the reply
	if rf.CurrentTerm != args.Term || rf.State != StateLeader {
		return
	}
	// if the follower's term is greater than the leader's term, step down to follower
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.State = StateFollower
		rf.VotedFor = -1
		rf.persist()
		return
	}

	// Update the nextIndex and matchIndex for the follower
	if Leader_LastIncludedIndex > rf.MatchIndex[server] {
		rf.MatchIndex[server] = Leader_LastIncludedIndex
		rf.NextIndex[server] = Leader_LastIncludedIndex + 1
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.Mu.Lock()
	if rf.State != StateLeader {
		rf.Mu.Unlock()
		return
	}
	if rf.NextIndex[server] <= rf.LastIncludedIndex {
		rf.Mu.Unlock()
		go rf.sendSnapshotToFollower(server)
		return
	}
	// Prepare args inside the lock
	prevLogIndex := rf.NextIndex[server] - 1
	// Now log is not 0-indexed effectively. Log[0] corresponds to index LastIncludedIndex, so we need to adjust the index accordingly.
	prevLogTerm := rf.Log[prevLogIndex-rf.LastIncludedIndex].Term

	// Make a copy of entries to send. (rf.Log[rf.NextIndex[server]-rf.LastIncludedIndex:] contains all entries from nextIndex)
	// if rf.NextIndex[server] is greater than the last log index, rf.Log[rf.NextIndex[server]-rf.LastIncludedIndex:] will be an empty slice,
	// which is fine for heartbeats.
	entries := make([]LogEntry, len(rf.Log[rf.NextIndex[server]-rf.LastIncludedIndex:]))
	copy(entries, rf.Log[rf.NextIndex[server]-rf.LastIncludedIndex:])

	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.Me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.CommitIndex,
	}
	rf.Mu.Unlock() // Unlock before sending RPC

	reply := &AppendEntriesReply{}
	// 1. Call RPC without holding any locks!
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	// 2. Lock again to process the response
	rf.Mu.Lock()

	// 3. Very important: check if we are still the leader in the same term
	if rf.CurrentTerm != args.Term || rf.State != StateLeader {
		rf.Mu.Unlock()
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.State = StateFollower
		rf.VotedFor = -1
		rf.persist()
		rf.Mu.Unlock()
		return
	}

	// 4. Process the response and update nextIndex/matchIndex
	if reply.Success {
		rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.MatchIndex[server] = rf.NextIndex[server] - 1

		// If majority matchIndex >= N, and log[N].term == currentTerm, update commitIndex
		// N is the last log index we can safely commit, which is the minimum of matchIndex of majority and the last log index
		for N := rf.getLastLogIndex(); N > max(rf.CommitIndex, rf.LastIncludedIndex); N-- {
			matchCount := 1 // count self
			for j := 0; j < len(rf.Peers); j++ {
				if j != rf.Me && rf.MatchIndex[j] >= N {
					matchCount++
				}
			}
			if matchCount > len(rf.Peers)/2 && rf.Log[N-rf.LastIncludedIndex].Term == rf.CurrentTerm {
				rf.CommitIndex = N
				break
			}
		}
	} else {
		// reply.Xterm is -1, the follower's log is too short, so we can jump back to the last log index in the follower's log directly
		// otherwise, we can jump back to the first index of the conflicting term
		if reply.Xterm == -1 {
			rf.NextIndex[server] = reply.LogLastIndex
		} else {
			// If the follower's log contains the term, we can find the last index of that term in our log
			// and set nextIndex to that index + 1
			lastindex := len(rf.Log) - 1
			for lastindex > 0 && rf.Log[lastindex].Term != reply.Xterm {
				lastindex--
			}
			if rf.Log[lastindex].Term == reply.Xterm {
				rf.NextIndex[server] = lastindex + rf.LastIncludedIndex + 1
			} else {
				rf.NextIndex[server] = reply.LogFirstIndex
				// If the follower's log doesn't contain the term, we can jump back to LogFirstIndex directly
				// This is an optimization to avoid unnecessary backtracking
			}
		}
	}
	rf.Mu.Unlock() // Re-lock before the next iteration
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. Otherwise start the
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

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// if this server isn't the leader, return false
	isLeader = rf.State == StateLeader
	if !isLeader {
		return index, term, isLeader
	}
	term = rf.CurrentTerm

	// append the new command to the log
	newIndex := rf.getLastLogIndex() + 1
	rf.Log = append(rf.Log, LogEntry{
		Command: command,
		Term:    term,
		Index:   newIndex,
	})
	index = newIndex
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
	atomic.StoreInt32(&rf.Dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	// get the current value of rf.Dead atomically.
	// 0 means "not killed". 1 means "killed".
	z := atomic.LoadInt32(&rf.Dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.Mu.Lock()
		if rf.State == StateLeader {
			for i := 0; i < len(rf.Peers); i++ {
				if i == rf.Me {
					continue
				}

				// if the nextIndex for this follower is less than or equal to the last included index,
				// we need to send an InstallSnapshot RPC instead of AppendEntries
				if rf.NextIndex[i] <= rf.LastIncludedIndex {
					rf.Mu.Unlock() // Unlock before sending RPC
					// send InstallSnapshot RPC
					go rf.sendSnapshotToFollower(i)
					rf.Mu.Lock() // Re-lock before the next iteration
					continue
				}
				rf.Mu.Unlock() // Unlock before sending RPC
				go rf.sendAppendEntries(i)

				rf.Mu.Lock() // Re-lock before the next iteration
			}
			rf.Mu.Unlock() // Unlock before sleeping!
			time.Sleep(120 * time.Millisecond)
			continue
		}
		// sleep for a while to avoid busy loop when we're not the leader
		if time.Since(rf.LastHeartbeat) < rf.ElectionTimeout {
			rf.Mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// if we haven't received a heartbeat from the leader for electionTimeout, start an election
		rf.Mu.Unlock()
		go rf.sendRequestVote()
		time.Sleep(10 * time.Millisecond)
	}
}

// applyEntries should be called periodically or after commitIndex changes
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.Mu.Lock()
		if rf.SnapshotPending {
			applyMsg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.Snapshot_Byte,
				SnapshotTerm:  rf.LastIncludedTerm,
				SnapshotIndex: rf.LastIncludedIndex,
			}
			rf.SnapshotPending = false
			rf.LastApplied = rf.LastIncludedIndex
			rf.Mu.Unlock()
			rf.ApplyCh <- applyMsg
			continue
		}

		if rf.CommitIndex > rf.LastApplied {
			if rf.LastApplied < rf.LastIncludedIndex {
				// This shouldn't happen normally because InstallSnapshot sets LastApplied,
				// but just in case, we can just skip applying these deleted logs.
				rf.LastApplied = rf.LastIncludedIndex
			}

			if rf.CommitIndex <= rf.LastApplied {
				rf.Mu.Unlock()
				continue
			}

			// Copy entries to apply to avoid holding lock while sending on applyCh
			firstUnapplied := rf.LastApplied + 1
			lastCommit := rf.CommitIndex
			entries := make([]LogEntry, lastCommit-firstUnapplied+1)
			copy(entries, rf.Log[firstUnapplied-rf.LastIncludedIndex:lastCommit-rf.LastIncludedIndex+1])
			rf.Mu.Unlock()

			for _, entry := range entries {
				rf.Mu.Lock()
				if entry.Index <= rf.LastIncludedIndex {
					rf.Mu.Unlock()
					continue
				}
				rf.Mu.Unlock()

				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.ApplyCh <- applyMsg

				rf.Mu.Lock()
				if entry.Index > rf.LastApplied {
					rf.LastApplied = entry.Index
				}
				rf.Mu.Unlock()
			}
		} else {
			rf.Mu.Unlock()
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
	rf.Peers = peers
	rf.Persister = persister
	rf.Me = me
	rf.ApplyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 0)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	rf.Snapshot_Byte = nil
	rf.Temp_Snapshot_Byte = nil
	rf.State = StateFollower
	rf.LastHeartbeat = time.Now()
	// rf.Snapshot = persister.ReadSnapshot()

	// if the log is empty, put a dummy log entry at index 0 to make the log start from index 1,
	rf.Log = append(rf.Log, LogEntry{
		Command: "",
		Term:    rf.CurrentTerm,
		Index:   0,
	})

	// randomize election timeout to avoid split votes
	rf.ElectionTimeout = time.Duration(300+(rand.Int63()%200)) * time.Millisecond

	// for leader
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.NextIndex[i] = 1
		rf.MatchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.Snapshot_Byte = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to periodically apply committed entries
	go rf.applier()

	return rf
}
