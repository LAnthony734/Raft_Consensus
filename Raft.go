//
// Raft.go
//
// This file defines a Raft API for use as a module by a larger server/tester.
//
// Referencing a lab assignment adapted from MIT's 2021 6.824 course with the support of Frans Kaashoek,
// Robert Morris, and Nickolai Zeldovich:
//
// "Raft is a replicated state machine protocol. A replicated service (e.g., key/value database) achieves 
// fault tolerance by storing copies of its data on multiple replica servers. Replication allows the 
// service to continue operating even if some of its servers experience failures (crashes or a broken or 
// flaky network). The challenge is that failures may cause the replicas to hold differing copies
// of the data...
// ...Raft manages a service's state replicas, and in particular it helps the service sort out what the correct
// state is after failures. Raft implements state machine replication. It organizes client requests into a 
// sequence, called the log, and ensures that all the replicas agree on the contents of the log. Each replica 
// executes the client requests in the log in the order they appear in the log, applying those requests to 
// the replica's local copy of the service's state. Since all the live replicas see the same log contents, 
// they all execute the same requests in the same order, and thus continue to have identical service state. 
// If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue 
// to operate as long as at least a majority of the servers are alive and can talk to each other. If there is
// no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority 
// can communicate again."
//
// Copyright (c) - 2023 Luke Andrews
//
package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// ** General Struct and Type Definitions **
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Debug flag:
// Set to 'true' to enable debug print statements.
//
var DebugMode bool = false

//
// RaftState definition:
//
// Used as a member in the Raft struct to identify which state the Raft server is in.
//
type RaftState int

const ( // Possible Raft states
	Leader RaftState = iota
	Candidate
	Follower
	Offline
)

//
// Raft
//
// This struct encapsulates the attributes of a single Raft server.
//
// ** Field names must start with capital letters **
//
type Raft struct {
	//
	// Persistent state on all servers (written to stable storage before RPC handling):
	//
	CurrentTerm int	       // The latest term this server has seen (initialized to 0)
	VotedFor    int	       // The candidate ID that recieved this server's vote in current term
	Entries     []LogEntry // The array of log entries (first index is 1)

	//
	// Volatile state on all servers:
	//
	CommitIndex      int           // The index of highest log entry known to be committed (initialized to 0)
	LastApplied      int           // The index of highest log entry applied to state machine (initialized to 0)
	State            RaftState     // The state that this server is currently in (see definition)
	TimeoutStartTime time.Time     // The start time of this server's election timeout

	//
	// Volatile state on leaders (reinitialized after election):
	//
	NextIndex     []int         // The array of indices for each peer server of the next log entry to send 
				        	    // to that server (initialized to leader last log index + 1)
	MatchIndex    []int         // The array of indices for each peer server of the highest log entry known 
					            // to be replicated on that server (initialized to 0)
	Heartbeat     time.Duration // The duration between heartbeats

	//
	// Channels for Raft peer servers to communicate:
	//
	AppendTrigger chan struct{} // A notification channel for the service to trigger to leaders when new entries 
								// are to be appended (not used for Heartbeats). struct{} is used because it
								// consumes the least amount of space
	CommitTrigger chan struct{} // A notification channel for triggering when new entries are to be committed.
							    // Followers and leaders can trigger to themselves when they discover these 
								// entries, or leaders can indirectly trigger to followers by updating their 
								// CommitIndex and sending new AppendEntries RPCs

	//
	// Uncategorized members:
	//
	Mu        sync.Mutex          // The lock for protecting shared access to this server's State
	Me        int                 // The index of this server into the peers array
	Persister *Persister          // The object to hold this server's persisted State
	Peers     []*labrpc.ClientEnd // The RPC end points of all peer servers
	ApplyCh   chan ApplyMsg       // The channel for sending ApplyMsgs to the service
}

//
// LogEntry
//
// This struct encapsulates the attributes of a single log entry.
// LogEntrys are used by Raft structs via slices (vectors).
//
// ** Field names must start with capital letters **
//
type LogEntry struct {
	Command interface{} // The command to be executed
	Term    int         // The term in which the command was issued
}

//
// ApplyMsg
//
// This struct encapsulates attributes of a Message sent by Raft servers to the global service (or tester).
//
// As each Raft server becomes aware that successive log entries are committed, the server should send an 
// ApplyMsg to the service (or tester), via the ApplyCh passed to Make(). 
//
// Setting CommandValid to true indicates to the service (or tester) that the ApplyMsg contains a newly
// committed log entry. In Lab 3, other kinds of Messages will be sent (e.g. snapshots) on the ApplyCh,
// at which point more fields will be added to ApplyMsg for other uses where CommandValid may be set to false.
//
// ** Field names must start with capital letters **
//
type ApplyMsg struct {
	CommandValid bool		 // The flag indicating that this Messages contains a newly committed log entry
	Command      interface{} // The command that was committed
	CommandIndex int         // The index of the command in the server's log
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// ** Raft Server Creation, Persistance & Destruction **
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Make
//
// This function is used by a service (or tester) to create a Raft server. All the servers' Peers[] arrays have 
// the same order (see parameter definition below). This function must return quickly, so goroutines must be used 
// for any long-running work.
//
// 		[in]  Peers     - the array of peer server ports (including the newly created server)
//      [in]  Me        - the ID of the created server (and index into Peers[])
//      [in]  Persister - the Persister used to save the created server's persisted state (see struct definition)
//      [in]  ApplyCh   - the channel used to communicate ApplyMsg structs to the service (or tester)
//      [out] rf        - the newly created Raft server struct
//
func Make(Peers []*labrpc.ClientEnd, Me int, Persister *Persister, ApplyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	
	//
	// Raft struct initialization:
	//
	rf.CurrentTerm = 0
	rf.VotedFor    = -1
	rf.Entries     = make([]LogEntry, 1) // Lowest entry index = 1. Index 0 reserved to nil
	rf.State       = Follower
	rf.DPrintf("[T%d] Born: coming online\n", rf.CurrentTerm)

	rf.CommitIndex      = 0
	rf.LastApplied      = 0
	rf.Heartbeat        = time.Millisecond * 100
	rf.TimeoutStartTime = time.Now()

	rf.NextIndex     = make([]int, len(Peers)) 
	rf.MatchIndex    = make([]int, len(Peers))

	rf.AppendTrigger = make(chan struct{}, 10) // Unbuffered...arbitrary size
	rf.CommitTrigger = make(chan struct{}, 10) // Unbuffered...arbitrary size

	rf.Peers     = Peers
	rf.Persister = Persister
	rf.Me        = Me
	rf.ApplyCh   = ApplyCh

	//
	// Initialize from state persisted before a crash:
	//
	rf.ReadPersist(Persister.ReadRaftState())
	
	//
	// Begin running the server and return immediately:
	//
	go rf.RunTimer()

	go rf.SendApplyMsgs()

	return rf
}

//
// Persist
//
// This function saves the Raft server's persistent state to stable storage where it can be later
// retrieved after a crash and restart.
//
// ** The State is expected to be locked (rf.Mu) prior to calling this function. **
//
func (rf *Raft) Persist() {
	bytesBuffer := new(bytes.Buffer)              // *bytes.Buffer
	encoder     := labgob.NewEncoder(bytesBuffer) // *LabEncoder

	if encoder.Encode(rf.CurrentTerm) != nil ||
	   encoder.Encode(rf.VotedFor)    != nil ||
	   encoder.Encode(rf.Entries)     != nil {
		return // If any decoding error, ignore and return BEFORE any modifications
	} else {
		data := bytesBuffer.Bytes()

		rf.Persister.SaveRaftState(data)
	}
}

//
// ReadPersist
//
// This function restores previously persisted Raft server state.
// No lock should be necessary, as this function should only be called by Make()
// (and only once) where no synchronization problems should be present.
//
//		[in] data - the raw byte array of stored data to be decoded
//
func (rf *Raft) ReadPersist(data []byte) {
	//
	// If there is no persisted data to read, simply return:
	//
	if data == nil || len(data) < 1 {
		return
	}

	//
	// Otherwise, decode data and load into Raft:
	//
	bytesBuffer := bytes.NewBuffer(data)          // *Buffer
	decoder     := labgob.NewDecoder(bytesBuffer) // *LabDecoder

	var currentTerm int
	var votedFor    int
	var entries     []LogEntry

	if decoder.Decode(&currentTerm) != nil ||
	   decoder.Decode(&votedFor)    != nil ||
	   decoder.Decode(&entries)     != nil {
		return // If any decoding error, ignore and return BEFORE any modifications
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor    = votedFor
		rf.Entries     = entries
	}
}

//
// Kill
//
// This function "kills" the Raft peer, leading to termination of its execution. The tester calls 
// this function when a peer is no longer needed.

// ** You are not required to do anything in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.Mu.Lock()

	rf.State = Offline
	close(rf.CommitTrigger)

	rf.Mu.Unlock()

	rf.DPrintf("[T%d] Killed: going offline\n", rf.CurrentTerm)
}

//
// GetState 
//
// This function gets the Raft server's current "state".
// ** Not to be confused with RaftState **
//
// 		[out] <int>  - the Raft server's current term
//      [out] <bool> - a flag indicating if the Raft server believes it's the leader
//
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.State == Leader
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// ** Raft Server Execution **
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// RequestVoteArgs
//
// This struct encapsulates arguments sent with a RequestVote RPC.
// The RPC will use values in this struct during execution.
//
// ** Field names must start with capital letters **
//
type RequestVoteArgs struct {
	Term         int // The candidate's term (should be incremented prior to call)
	CandidateID  int // The candidate's ID
	LastLogIndex int // The index of the candidate's last log entry
	LastLogTerm  int // The term of the candidate's last log entry
}

//
// RequestVoteReply
//
// This struct encapsulates the reply sent with a RequestVote RPC.
// The RPC will write values to this struct during execution.
//
// ** Field names must start with capital letters **
//
type RequestVoteReply struct {
	Term        int  // The term that the candidate should update itself to
	VoteGranted bool // The flag that indicates if the candidate received this peer's vote 
}

//
// AppendEntriesArgs
//
// This struct encapsulates arguMents sent with an AppendEntries RPC.
// The RPC will use values in this struct during execution.
//
// ** Field names must start with capital letters **
//
type AppendEntriesArgs struct {
	Term         int        // The leader's term
	LeaderID     int        // The leader's ID
	PrevLogIndex int        // The index of the log entry immediately preceding new ones
	PrevLogTerm  int        // The term of the PrevLogIndex entry
	Entries      []LogEntry // The array of log entries to store (or empty for Heartbeats)
	LeaderCommit int        // The leader's commit index   
}

//
// AppendEntriesReply
//
// This struct encapsulates the reply sent with an AppendEntries RPC.
// The RPC will write values to this struct during execution.
//
// ** Field names must start with capital letters **
//
type AppendEntriesReply struct {
	Term          int  // The term that the leader should update itself to
	Success       bool // The flag indicating whether the follower contained an entry matching 
	                   // PrevLogIndex and PrevLogTerm
	Error         bool // The flag indicating whether an error has occured when appending
	ConflictTerm  int  // The term of the entry in the follower's log that fails the consistency check
	ConflictIndex int  // The index of the earliest appearance of ConflictTerm in the follower's log
}

//
// BecomeLeader
//
// This function transitions a Raft server to a leader State, handling all State changes
// and behavior. A timer is used to send Heartbeats every 100 milliseconds.
//
// ** The State is expected to be locked (rf.Mu) prior to calling this function. **
//
func (rf *Raft) BecomeLeader() {
	rf.State = Leader
	rf.DPrintf("[T%d] State changed to leader...\n", rf.CurrentTerm)

	for peerIndex, _ := range rf.Peers {
		rf.NextIndex[peerIndex]  = len(rf.Entries) // Log last index + 1
		rf.MatchIndex[peerIndex] = 0
	}

	go func() {
		rf.SendAppendEntries(true) // Send initial Heartbeat

		timer := time.NewTimer(rf.Heartbeat)
		defer timer.Stop()	

		for true {
			rf.DPrintf("[T%d] Leader looping...\n", rf.CurrentTerm)
			sendAppend  := false
			isHeartbeat := false

			select {
				//
				// Timer expired. No new entries, so send another heartbeat:
				//
				case <- timer.C:
					rf.DPrintf("[T%d] Leader timer: send heartbeat\n", rf.CurrentTerm)
					sendAppend  = true
					isHeartbeat = true
					timer.Stop()
					timer.Reset(rf.Heartbeat)
				//
				// New entry triggered before time expired. Send append new entries:
				//
			    case _, isOpen := <- rf.AppendTrigger:
					if isOpen {
						rf.DPrintf("[T%d] Leader timer: send new entry\n", rf.CurrentTerm)
						sendAppend = true
					} else {
						return
					}
					
					if !timer.Stop() { // If the timer already stopped, clear the channel:
						<- timer.C 
					}
					timer.Reset(rf.Heartbeat)
			}

			if sendAppend == true {
				//
				// Verify that this server is still the leader before appending. If not, return:
				//
				rf.DPrintf("[T%d] Leader acquiring lock for appending...\n", rf.CurrentTerm)
				rf.Mu.Lock()
				rf.DPrintf("[T%d] Leader acquired lock for appending\n", rf.CurrentTerm)
				if rf.State != Leader {
					rf.Mu.Unlock()
					return
				}
				rf.Mu.Unlock()

				rf.DPrintf("[T%d] Leader sending appends (HB = %v)...\n", rf.CurrentTerm, isHeartbeat)
				rf.SendAppendEntries(isHeartbeat)
			}
		}
	}()
}

//
// BecomeFollower
//
// This function transitions a Raft server to a follower state, handling all state changes
// and behavior.
//
//		[in] newCurrentTerm - the term that the raft server is to update to
//
// ** The State is expected to be locked (rf.Mu) prior to calling this function. **
//
func (rf *Raft) BecomeFollower(newCurrentTerm int) {
	rf.State            = Follower
	rf.DPrintf("[T%d] State changed to follower...\n", rf.CurrentTerm)
	rf.CurrentTerm      = newCurrentTerm
	rf.VotedFor         = -1
	rf.TimeoutStartTime = time.Now()
	rf.Persist()
	
	go rf.RunTimer()
}

//
// RunTimer
//
// This function handles the election timer of a Raft server. The election timeout is handled via
// an infinite loop. A start time and a ticker is initialized prior to looping, and the time is checked
// periodically within the loop. The ticker is only used to slow down the loop to checking only once
// every ten milliseconds. Upon the time duration exceeding the server's timeout duration, the
// start time is reset, an election is launched and the function returns. Additional checks are performed 
// inside the loop prior to checking the time to verify that the server is not unnecessarily using the timer.
//
// ** Implementation Notes: **
// I saw a problem with looping so frequently and having to perform so many checks, so I wanted to slow down
// the loop. Initially, I planned on calling time.Sleep() for 10 milliseconds, but figured performing that
// many context switches was grossly more inefficient than simply being blocked on a channel. For this
// reason, Ticker seemed like the right solution. Also, if I interpretted the documentation correctly, the
// Ticker compensates for slow receivers by adjusting its intervals or dropping ticks, and this sounded
// desirable. 
//
func (rf *Raft) RunTimer() {
	timeoutDuration := rf.NewTimeoutDuration()

	rf.Mu.Lock()
	startingTerm := rf.CurrentTerm
	rf.Mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for true {
		<- ticker.C // Block - wait for Ticker

		rf.Mu.Lock()

		//
		// If the State of the server is not a Candidate or a Follower, it is inferred that the timer
		// is no longer needed. Simply release the server's lock and return.
		//
		if rf.State != Candidate && rf.State != Follower {
			rf.Mu.Unlock()
			return
		}

		//
		// If the server's current term has changed since the start of the loop, it is inferred that
		// the timer is no longer needed. Simply release the server's lock and return.
		//
		if rf.CurrentTerm != startingTerm {
			rf.Mu.Unlock()
			return
		}

		//
		// Check the timer. If timeout duration is met, run election, release the server's lock
		// and return.
		//
		timeElapsed := time.Since(rf.TimeoutStartTime)

		if timeElapsed >= timeoutDuration {
			rf.RunElection()
			rf.Mu.Unlock()
			return
		}

		rf.Mu.Unlock()
	}
}

//
// NewTimeoutDuration
//
// This function randomly generates a new timeout duration for a raft server
// between 0.75 and 1.25 seconds in millisecond intervals.
//
// ** Implementation Notes: **
// I originally had timeout durations hard-set in the Make() function. However, I experienced
// in 2B a unique case of two followers with no leader. The follower with the less complete log
// would always timeout first because its duration was always shorter and would be denied the other
// follower's vote each time it asked (consistent with the voting rules). To avoid this infinite loop,
// I figured this function was the right approach to let the other follower eventually win the race.
// Regarding the interval, the tests had a hard-set one-second duration, so I worked around that.
//
// 		[out] <time.Duration> - the generated timeout duration
//
func (rf *Raft) NewTimeoutDuration() time.Duration {
	timeoutDuration := time.Duration(rand.Intn(500) + 750) * time.Millisecond

	return timeoutDuration
}

//
// RunElection
//
// This function handles a new election run by a candidate. It does not return until all peer servers
// are either sent a RequestVote RPC or the candidate wins the election before then.
//
// ** The State is expected to be locked (rf.Mu) prior to calling this function. **
//
func (rf *Raft) RunElection() {
	rf.State       = Candidate
	rf.DPrintf("[T%d] State changed to candidate...\n", rf.CurrentTerm)
	rf.VotedFor    = rf.Me
	rf.CurrentTerm++

	rf.DPrintf("[T%d] Starting Election\n", rf.CurrentTerm)

	currentTerm       := rf.CurrentTerm
	votesReceived     := 1
	voteMajority      := (len(rf.Peers) / 2) + 1

	rf.TimeoutStartTime = time.Now()

	//
	// For each peer (that isn't this server), send a RequestVote RPC in a goroutine:
	//
	for peerIndex, _ := range rf.Peers {
		if peerIndex != rf.Me {
			go func(peerIndexArg int) {
				//
				// Prepare argument and reply struct for RPC:
				//
				rf.Mu.Lock()
				lastLogEntryIndex := len(rf.Entries) - 1
				lastLogEntryTerm  := rf.Entries[lastLogEntryIndex].Term
				rf.Mu.Unlock()

				args  := RequestVoteArgs{currentTerm, rf.Me, lastLogEntryIndex, lastLogEntryTerm}
				reply := RequestVoteReply{}

				//
				// Make and handle results of RPC:
				//
				rf.DPrintf("[T%d] Sent a vote request to S%d...\n", currentTerm, peerIndexArg)
				callSucceeded := rf.Peers[peerIndexArg].Call("Raft.RequestVote", &args, &reply)
				
				if callSucceeded { // Ignore otherwise (RPC error)
					rf.Mu.Lock()
					defer rf.Mu.Unlock()

					//
					// If this Raft server is not a candidate, return false. It's possible that something 
					// could have gone wrong, but just ignore the possibility and treat it as a denied vote.
					// It's more likely that the candidate became a follower earlier in the election:
					//
					if rf.State != Candidate {
						return
					}

					//
					// If the candidate's term is now stale, revert to follower and return false:
					//
					if reply.Term > currentTerm {
						rf.BecomeFollower(reply.Term)
						return
					//
					// Else if the candidate's term matches the reply and the vote was granted:
					//
					} else if reply.Term == currentTerm && reply.VoteGranted == true {
						rf.DPrintf("[T%d] Vote granted by S%d!\n", currentTerm, peerIndexArg)
						votesReceived++

						//
						// If the candidate received a majority vote, they win the election.
						// Become leader and return:
						//
						if votesReceived >= voteMajority {
							rf.DPrintf("[T%d] I WON THE ELECTION\n", currentTerm)
							rf.BecomeLeader()
							return
						}
					}
				}

			}(peerIndex)
		}
	}

	go rf.RunTimer()
}

//
// *DEPRECATED* SendRequestVote
//
// This function handles the sending of RequestVote RPCs by a candidate server. It forwards a struct of 
// arguments to a single voter server to determine results and a reply struct for that server to fill out. 
// Upon return of the RPC, this function handles any necessary actions regarding the candidate server's 
// state based on the resulting reply struct.
//
//		[in]  server - the index (ID) of the peer server to request a vote from
//      [in]  args   - the arguments struct
//      [in]  reply  - the reply struct
//      [out] <bool> - whether the candidate is granted the vote
//
// func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	callSucceeded := rf.Peers[server].Call("Raft.RequestVote", args, reply)
//
// 	if callSucceeded { // Ignore otherwise (RPC error)
// 		rf.Mu.Lock()
// 		defer rf.Mu.Unlock()
//
// 		//
// 		// If this Raft server is not a candidate, return false. It's possible that something 
// 		// could have went wrong, but just ignore the possibility and treat it as a denied vote.
// 		// It's more likely that the candidate becaMe a follower earlier in the election:
// 		//
// 		if rf.State != Candidate {
// 			return false
// 		}
//
// 		//
// 		// If the candidate's term is now stale, revert to follower and return false:
// 		//
// 		if reply.Term > args.Term {
// 			rf.BecomeFollower(reply.Term)
// 			return false
// 		}
//
//      //
//      // If the follower's term is still smaller than the candidate's, return false:
//      // (the term should have been updated during the RPC call)
//      //
//      if reply.Term < args.Term {
// 			return false
// 		}
//
// 		//
// 		// If the candidate's term matches the reply, return whether the vote was granted:
// 		//
// 		if reply.Term == args.Term {
// 			return reply.VoteGranted
// 		}
// 	}
//
// 	return false
// }

//
// RequestVote
//
// This function handles an incoming vote request from a candidate peer server. This server (voter) is
// passed a RequestVoteArgs struct to use when determining its vote, as well as a RequestVoteReply struct 
// to fill for the caller. This function does not take action regarding the server's state, but rather 
// provides the caller information needed to take that action.
//
// 		[in] args  - the struct of arguments passed by the candidate
//      [in] reply - the struct of replies passed by the candidate
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	//
	// Default reply values - use voter's term and don't grant vote:
	//
	reply.Term        = rf.CurrentTerm
	reply.VoteGranted = false

	//
	// If the voter is in an offline state, return:
	//
	if rf.State == Offline {
		rf.DPrintf("[T%d] Voter offline\n", rf.CurrentTerm)
		return
	}

	//
	// If the candidate is in a stale term, return:
	//
	if rf.CurrentTerm > args.Term {
		rf.DPrintf("[T%d] Voter higher term\n", rf.CurrentTerm)
		return
	}

	//
	// If the voter is in a stale term, become a follower, update reply to new term and return:
	//
	if rf.CurrentTerm < args.Term {
		rf.DPrintf("[T%d] Voter becoming follower\n", rf.CurrentTerm)
		rf.BecomeFollower(args.Term)
		reply.Term = rf.CurrentTerm // Reset - changed after BecomeFollower()
	}
	
	//
	// If the follower has already voted for another candidate, return:
	// (assumes terms are equal at this point)
	//
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID {
		rf.DPrintf("[T%d] Voter already voted for S%d\n", rf.CurrentTerm, rf.VotedFor)
		return
	}

	//
	// If the term of the voter's last log entry is greater than the candidate's, return:
	//
	voterLastEntry := rf.Entries[len(rf.Entries) - 1]

	if voterLastEntry.Term > args.LastLogTerm {
		rf.DPrintf("[T%d] Voter last log entry term higher\n", rf.CurrentTerm)
		return
	}

	//
	// If the term of the voter's last log entry is equal to the candidate's, but
	// the voter's log is longer, return:
	//
	if voterLastEntry.Term == args.LastLogTerm && len(rf.Entries) > args.LastLogIndex + 1 {
		rf.DPrintf("[T%d] Voter last log entry term equals, but log longer\n", rf.CurrentTerm)
		return
	}

	//
	// Otherwise, grant vote:
	//
	rf.DPrintf("[T%d] Voter granted vote to %d\n", rf.CurrentTerm, args.CandidateID)
	reply.VoteGranted   = true
	rf.VotedFor         = args.CandidateID
	rf.Persist()
	rf.TimeoutStartTime = time.Now()

	return
}

//
// SendAppendEntries
//
// This function handles the sending of AppendEntries RPCs by a leader server. It provides a struct of 
// arguments to all follower servers to determine results and a reply struct for thoses servers to fill out. 
// Upon return of the RPC, this function handles any actions regarding the leader server's state based on 
// the resulting reply structs.
//
func (rf *Raft) SendAppendEntries(isHeartbeat bool) {
	rf.Mu.Lock()

	if rf.State != Leader {
		rf.Mu.Unlock()
		return
	}

	currentTerm := rf.CurrentTerm // Save current term for all AppendEntries calls

	rf.Mu.Unlock()

	for peerIndex, _ := range rf.Peers {
		if peerIndex != rf.Me {
			go func(peerIndex int){
				//
				// Create arguments and reply structs:
				//
				rf.Mu.Lock()

				prevLogIndex := rf.NextIndex[peerIndex] - 1

				if prevLogIndex < 0 {
					prevLogIndex = 0
				}

				prevLogTerm := 0

				if prevLogIndex > 0 {
					prevLogTerm = rf.Entries[prevLogIndex].Term
				}

				appendEntries := rf.Entries[prevLogIndex + 1 : ]

				args := AppendEntriesArgs{Term:         currentTerm, 
										  LeaderID:     rf.Me,
										  PrevLogIndex: prevLogIndex,
										  PrevLogTerm:  prevLogTerm,
										  Entries:      appendEntries,
										  LeaderCommit: rf.CommitIndex}
				
				rf.Mu.Unlock()

				reply := AppendEntriesReply{}

				//
				// Make and handle append entries call:
				//
				callSucceeded := rf.Peers[peerIndex].Call("Raft.AppendEntries", &args, &reply)

				if callSucceeded { // Ignore otherwise (RPC error)
					rf.Mu.Lock()

					//
					// If term is now stale, become follower and return:
					//
					if reply.Term > rf.CurrentTerm {
						rf.DPrintf("[T%d] Leader Becoming follower: AppendEntries\n", rf.CurrentTerm)
						rf.BecomeFollower(reply.Term)
						rf.Mu.Unlock()
						return
					}

					//
					// If still a leader and terms are equal, check if append succeeded:
					//
					if rf.State == Leader && reply.Term == currentTerm {
						//
						// If append succeeded, update leader:
						//
						if reply.Success == true {
							rf.NextIndex[peerIndex]  = prevLogIndex + len(appendEntries) + 1
							rf.MatchIndex[peerIndex] = rf.NextIndex[peerIndex] - 1

							//
							// For each entry in the leader's log, if it was added in the leader's
							// term, check how many followers also have it in their logs. If a majority
							// of followers do, consider it committed by setting the commit index to
							// the entry's index:
							//
							commitIndex  := rf.CommitIndex
							peerMajority := (len(rf.Peers) / 2) + 1

							for logIndex := rf.CommitIndex + 1; logIndex < len(rf.Entries); logIndex++ {
								if rf.Entries[logIndex].Term == rf.CurrentTerm {
									matchCount := 1

									// peerJndex to not overwrite peerIndex
									for peerJndex, _ := range rf.Peers {
										if rf.MatchIndex[peerJndex] >= logIndex {
											matchCount++
										}
									}

									if matchCount >= peerMajority {
										rf.CommitIndex = logIndex
									}
								}
							}
							
							//
							// If this leader's commit index has changed, then there are new entries to
							// commit. Send to the CommitTrigger channel to notify self to commit the
							// new entries, and send to AppendTrigger channel to indirectly notify
							// followers to commit the new entries:
							//
							if rf.CommitIndex != commitIndex {
								rf.DPrintf("[T%d] Leader commit index changed: notifying followers\n", rf.CurrentTerm) 
								rf.CommitTrigger <- struct{}{}
								rf.AppendTrigger <- struct{}{}
							}
						//
						// Otherwise, if no error, update NextIndex for later re-call:
						//
						} else if reply.Error == false {
							//
							// If the ConflictTerm is valid, calculate NextIndex accordingly:
							//
							if reply.ConflictTerm > 0 {
								var logIndex int

								//
								// Locate the last occurrence of ConflictTerm in the leader's log:
								//
								for logIndex = len(rf.Entries) - 1; logIndex > 0; logIndex-- {
									if rf.Entries[logIndex].Term == reply.ConflictTerm {
										break
									}
								}

								//
								// If logIndex is greater than zero, the leader contained an entry for the
								// returned ConflictTerm:
								//
								if logIndex > 0 {
									rf.NextIndex[peerIndex] = logIndex + 1
								} else {
									rf.NextIndex[peerIndex] = reply.ConflictIndex
								}
							//
							// Otherwise, the provided PrevLogIndex was too large for the follower's log:
							//
							} else {
								rf.NextIndex[peerIndex] = reply.ConflictIndex
							}
						}
					}

					rf.Mu.Unlock()
				}
			}(peerIndex)
		}
	}
}

//
// AppendEntries
//
// This function handles an incoming append entries request from a leader peer server. The caller passes
// an AppendEntriesArgs struct for the callee to use when appending, as well as an AppendEntriesReply 
// struct to fill for the caller. This function does not take action regarding the server's State, but rather 
// provides the caller information needed to take that action.
//
// 		[in] args  - the struct of arguments passed by the leader
//      [in] reply - the struct of replies passed by the leader
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	//
	// Default reply values:
	//
	reply.Term    = rf.CurrentTerm
	reply.Success = false
	reply.Error   = false

	//
	// Validity Check: If this server is offline, return error:
	//
	if rf.State == Offline {
		rf.DPrintf("[T%d] Append error: Server offline\n", rf.CurrentTerm)
		reply.Error = true
		return
	}

	//
	// Validity Check: If the leader's provided PrevLogIndex is less than zero, return error.
	// Note: at minimum, len(rf.Entries) should return 1, in which case args.PrevLogIndex would
	// be valid at a maximum value of 0. Because args.PrevLogIndex is progressively decremented until
	// the consistency checks succeeds, should the value be less than zero, something has gone wrong:
	//
	if args.PrevLogIndex < 0 {
		rf.DPrintf("[T%d] Append error: PrevLogIndex too small (%d)\n", rf.CurrentTerm, args.PrevLogIndex)
		reply.Error = true
		return
	}

	//
	// Validity Check: If the leader's term is smaller than this server's, return error:
	//
	if args.Term < rf.CurrentTerm {
		rf.DPrintf("[T%d] Append error: Stale leader term (T%d)\n", rf.CurrentTerm, args.Term)
		reply.Error = true
		return
	}

	//
	// Follower Check: If the leader's term is larger than this server's, become a follower
	// with the leader's term (ultimately resetting the follower's timeout start time). 
	// Otherwise, simply reset the follower's timeout start time manually.
	// Then, continue:
	//
	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.State != Follower) {
		rf.BecomeFollower(args.Term)
		rf.Persist()
	} else {
		rf.TimeoutStartTime = time.Now()
	}

	//
	// Log Consistency Check: If the leader's provided PrevLogIndex is too large for this server's log,
	// adjust the reply ConflictTerm and ConflictIndex as necessary and return:
	// (assumed that terms are equal at this point)
	//
	if args.PrevLogIndex > len(rf.Entries) - 1 {
		rf.DPrintf("[T%d] Consistency error: PrevLogIndex too large (%d)\n", rf.CurrentTerm, args.PrevLogIndex)
		reply.ConflictTerm  = 0
		reply.ConflictIndex = len(rf.Entries)
		return
	}

	//
	// Log Consistency Check: If the leader's provided PrevLogIndex is greater than zero and its provided
	// PrevLogTerm does not match the term of this server's log entry at PrevLogIndex, adjust the reply
	// ConflictTerm and ConflictIndex as necessary and return:
	//
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.Entries[args.PrevLogIndex].Term {
		rf.DPrintf("[T%d] Consistency error: PrevLogTerm not equal\n", rf.CurrentTerm)

		conflictTerm := rf.Entries[args.PrevLogIndex].Term

		var logIndex int

		for logIndex = args.PrevLogIndex - 1; logIndex > 0; logIndex-- {
			if rf.Entries[logIndex].Term != conflictTerm {
				break
			}
		}

		reply.ConflictTerm  = conflictTerm
		reply.ConflictIndex = logIndex + 1

		return
	}

	//
	// After all checks, handle the entries append:
	//
	if args.Entries != nil {
		rf.Entries = append(rf.Entries[ : args.PrevLogIndex + 1], args.Entries...)
		rf.Persist()

		if len(rf.Entries) > 1 {
			rf.DPrintf("[T%d] Appended entries from S%d   (N = %d, last = %d)\n", rf.CurrentTerm, args.LeaderID, 
						len(rf.Entries) - 1, rf.Entries[len(rf.Entries) - 1].Command.(int))
		} else {
			rf.DPrintf("[T%d] Appended entries from S%d   (N = 0, last = nil)\n", rf.CurrentTerm, args.LeaderID) 
		}
	} else {
		if len(rf.Entries) > 1 {
			rf.DPrintf("[T%d] Recieved heartbeat from S%d (N = %d, last = %d)\n", rf.CurrentTerm, args.LeaderID, 
						len(rf.Entries) - 1, rf.Entries[len(rf.Entries) - 1].Command.(int))
		} else {
			rf.DPrintf("[T%d] Recieved heartbeat from S%d (N = 0, last = nil)\n", rf.CurrentTerm, args.LeaderID) 
		}
	}
	
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = IntMin(args.LeaderCommit, len(rf.Entries) - 1)
		rf.DPrintf("[T%d] Follower commit index changed: triggering commit\n", rf.CurrentTerm)
		rf.CommitTrigger <- struct{}{}
	}

	reply.Success = true

	return	
}

//
// Start
//
// This function is used by the service (or tester) to start an agreement on the next command to be 
// appended to a Raft server's log. If this server isn't the leader, returns false. Otherwise, starts 
// the agreement and return immediately. There is no guarantee that this command will ever be committed 
// to the Raft server's log since the leader may fail or lose an election. Even if the Raft instance has 
// been killed, this function should return gracefully.
//
//		[in]  command  - the command (as an interface{}) to be appended to the log
//      [out] index    - the index that the command will appear at if it's ever committed
//      [out] term     - the Raft server's current term
//      [out] isLeader - the flag indicating if the Raft server believe's it is the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Mu.Lock()
	
	index    := -1
	term     := -1
	isLeader := false    

	if rf.State == Leader {
		rf.DPrintf("[T%d] Leader recieved new command (%d) \n", rf.CurrentTerm, command.(int))
		index      = len(rf.Entries)
		term       = rf.CurrentTerm
		isLeader   = true	
		rf.Entries = append(rf.Entries, LogEntry{Command: command, Term: term})
		rf.Persist()
		rf.Mu.Unlock()
		rf.AppendTrigger <- struct{}{}
	} else {
		rf.Mu.Unlock()
	}

	return index, term, isLeader
}

//
// SendApplyMsgs
//
// This function loops through "triggers" in the server's CommitTrigger channel to create any new
// ApplyMsg structs to send to the server's ApplyCh channel. If the CommitTrigger channel is empty,
// the loop will block. The loop only breaks, and ultimately the function only returns, once
// the CommitTrigger channel is closed.
//
func (rf *Raft) SendApplyMsgs() {
	for range rf.CommitTrigger {
		//
		// Determine which Entries to commit:
		//
		rf.DPrintf("[T%d] S%d acquiring lock (SendApplyMsgs)\n", rf.CurrentTerm, rf.Me)
		rf.Mu.Lock()
		rf.DPrintf("[T%d] S%d acquired lock (SendApplyMsgs)\n", rf.CurrentTerm, rf.Me)

		LastApplied := rf.LastApplied

		var commitEntries []LogEntry

		if rf.CommitIndex > rf.LastApplied {
			rf.DPrintf("[T%d] New entry committed\n", rf.CurrentTerm)
			commitEntries  = rf.Entries[rf.LastApplied + 1 : rf.CommitIndex + 1]
			rf.LastApplied = rf.CommitIndex
		}

		rf.DPrintf("[T%d] S%d releasing lock (SendApplyMsgs)\n", rf.CurrentTerm, rf.Me)
		rf.Mu.Unlock()
		rf.DPrintf("[T%d] S%d released lock (SendApplyMsgs)\n", rf.CurrentTerm, rf.Me)

		//
		// Create and send new ApplyMsg structs for each newly committed entry:
		//
		for i, entry := range commitEntries {
			rf.ApplyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: LastApplied + i + 1}
		}
	}
}

//
// IntMin
//
// This function returns the smaller of two integers. Ties are handled by the first integer being
// smaller than the second.
//
// ** Implementation Notes: **
// The Min function in the math package returns a float64, which is not what I wanted. 
// Rather than casting that value to an int, I just created this function instead.
//
func IntMin(intOne int, intTwo int) int {
	if intOne <= intTwo {
		return intOne
	} else {
		return intTwo
	}
}

//
// DPrintf
//
// Printf for debugging mode. See DebugMode boolean flag at the top of this file.
//
//	format - the string to print with defined Printf formatting specifiers
//  args   - the list of values supplied to the formatting specifiers
//
// ** Implementation Notes: **
// I created this function before noticing the already-provided function in util.go.
// Rather than replace every call to this function within raft.go with the util.go
// version, I decided to save time and just keep this in.
//
func (rf *Raft) DPrintf(format string, args ...interface{}) {
	if DebugMode == true {
		format = fmt.Sprintf("[%d] ", rf.Me) + format
		fmt.Printf(format, args...)
	}
}