package main

type Log struct {
    term      int
    idx       int
    committed bool
    newState  StateMachine
}

type StateMachine struct {
    x int
    y int 
    z int
}

type Persister struct {
	currentTerm int
	votedFor    int
	logs        []Log
}

type Follower struct {
	commitIndex int
	lastApplied int
}

type Leader struct {
	nextIndex []int
	matchIndex []int
}

func MakePersister() *Persister {
	return &Persister{-1, -1, nil}
}

func (ps *Persister) MakeLeader(me *Follower, id int) *Leader {
	leader := Leader{}
	leader.nextIndex = make([]int, numNodes)
	leader.matchIndex = make([]int, numNodes)

	for i := 0; i < id; i++ {
		if i != id {
			leader.nextIndex[i] = me.commitIndex + 1
			leader.matchIndex[i] = 0
		}
	}

	return &leader
}

// Can be used as initialization or recovery
func (ps *Persister) MakeFollower() *Follower {
	if (ps.logs == nil || ps.currentTerm == -1) {
		ps.currentTerm = 0
		ps.logs = make([]Log, 1)

		ps.logs[0] = Log{0, 0, false, StateMachine{0,0,0}}

		return &Follower{0, 0}
	}

	for i := len(ps.logs); i > 0; i-- {
		if (ps.logs[i].committed) {
			return &Follower{i, len(ps.logs)}
		}
	}

	return &Follower{0, 0}
}

// returns true if able to add to log, false otherwise
func (ps *Persister) FollowerRecieveLog(me *Follower, log Log, prev Log) bool {
	if prev.idx == ps.logs[me.lastApplied].idx && prev.term == ps.logs[me.lastApplied].idx {
		ps.logs = append(ps.logs, log)
		ps.logs[me.lastApplied].committed = true
		
		me.commitIndex++
		me.lastApplied++

		return true
	}

	return false
}

// Assumes new log to be sent has been added, but not committed locally
func (ps *Persister) SendLogs(me *Leader, peers []chan Message, id int) {
	for i := 0; i < numNodes; i++ {
		if (i != id) {
			logIdx := me.nextIndex[i]

			peers[i] <- Message{LOG, id, ps.logs[logIdx], ps.logs[logIdx - 1]}
		}
	}
}

func (ps *Persister) LeaderRecieveLogAck(me *Leader, msg Message) {
	
}

func (ps *Persister) UpdateCommitted() {

}

