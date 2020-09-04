// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// Raft is a structure holding the state of a raft node
type Raft struct {
	id uint64

	Term uint64

	// Voted for
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// Leader's heartbeat interval, should send a heartbeat message
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// initial election timeout, used as reference to randomization
	initialElectionTimeout int

	// peers contains the IDs of all nodes in the raft cluster including itself
	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	votes := make(map[uint64]bool, len(c.peers)-1)
	for _, p := range c.peers {
		votes[p] = false
	}
	raft := &Raft{
		id:                     c.ID,
		Term:                   0,
		Vote:                   None,
		RaftLog:                newLog(c.Storage),
		State:                  StateFollower,
		votes:                  votes,
		electionTimeout:        c.ElectionTick,
		initialElectionTimeout: c.ElectionTick,
		heartbeatTimeout:       c.HeartbeatTick,
		electionElapsed:        0,
		heartbeatElapsed:       0,
		peers:                  c.peers,
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// No need to send heartbeat to oneself
	if to == r.id {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	r.heartbeatElapsed++
	switch r.State {
	case StateLeader:
		// If heartbeat timed out, send heart beat to all peers
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.leaderSendHeartbeat()
		}
	case StateFollower:
		if r.electionElapsed%r.electionTimeout == 0 {
			r.electionElapsed = 0
			r.becomeCandidate()
			r.requestVoteRPC()
		}
	case StateCandidate:
		// election timeout: start a new election
		if r.electionElapsed%r.electionTimeout == 0 {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	// Randomize election timeout`
	r.electionTimeout = r.initialElectionTimeout + rand.Intn(r.initialElectionTimeout)
	// clear previous state:
	r.votes = make(map[uint64]bool)
	// Increment current term
	r.Term++
	// Vote for oneself
	r.Vote = r.id
	r.votes[r.id] = true
	// Reset election timer
	r.electionElapsed = 0
	// Check if the candidate can become a leader
	if len(r.peers) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
}

// Step is the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// All servers rule 2: update term
	if m.Term > r.Term {
		r.Term = m.Term
		// clear state associating with previous term
		r.Vote = 0
		// outdate candidate/leader falls back
		if r.State == StateCandidate || r.State == StateLeader {
			r.State = StateFollower
		}
	}
	switch r.State {
	case StateFollower:
		// Reply false if term < currentTerm
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.respondRequestVoteRPC(&m)
		case pb.MessageType_MsgHeartbeat:
			r.respondHeartbeat(&m)
		case pb.MessageType_MsgHup:
			fmt.Println("election timeout: candidate starts a new election")
			r.becomeCandidate()
			// send RequestVote RPC
			r.requestVoteRPC()
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			return r.respondRequestVoteRPC(&m)
		case pb.MessageType_MsgRequestVoteResponse:
			if m.To == r.id && !m.Reject {
				r.votes[m.From] = true
				// Count votes
				count := 0
				for _, v := range r.votes {
					if v == true {
						count++
					}
				}
				if count > (len(r.peers))/2 {
					// r wins the vote
					r.becomeLeader()
				}
			}
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.requestVoteRPC()
		case pb.MessageType_MsgAppend:
			// Candidate Rule #4: convert to follower if receive Append from a nwe leader
			// MsgAppend indicates that the sender is a leader, we only need to check Term
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgPropose:
			// According to TestLeaderBcastBeat2AA
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgRequestVote:
			r.respondRequestVoteRPC(&m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// FIXME: should be f/c's method
// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.heartbeatElapsed = 0
	for _, id := range r.peers {
		r.sendHeartbeat(id)
	}
}

func (r *Raft) leaderSendHeartbeat() {
	r.heartbeatElapsed = 0
	for _, id := range r.peers {
		r.sendHeartbeat(id)
	}
}

func (r *Raft) respondHeartbeat(m *pb.Message) {
	r.electionElapsed = 0
	reject := false
	if m.Term < r.Term {
		reject = true
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) requestVoteRPC() {
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		lastLogTerm, err := r.RaftLog.Term(r.RaftLog.committed)
		if err != nil {
			fmt.Println("raft.go:becomeCandidate: Failed to get lastLogTerm")
			continue
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
			LogTerm: lastLogTerm,
			Index:   r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) respondRequestVoteRPC(m *pb.Message) error {
	reject := true
	// Is a's log at least as up-to-date as b?
	upToDate := func(aTerm uint64, aLen uint64, bTerm uint64, bLen uint64) bool {
		if aTerm > bTerm {
			return true
		} else if aTerm == bTerm && aLen >= bLen {
			return true
		}
		return false
	}
	nodeLastLogTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		return err
	}
	if m.Term >= r.Term && (r.Vote == None || r.Vote == m.From) && upToDate(m.GetLogTerm(), m.Index, nodeLastLogTerm, r.RaftLog.LastIndex()) {
		// If m.Term is not smaller then the node's term
		// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as r's log, grant vote
		reject = false
	}
	response := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, response)
	if !reject {
		r.Vote = m.From
	}
	return nil
}
