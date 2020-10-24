package main

import (
    "fmt"
    "math/rand"
    "sync"
    "time"
)

const (
    numNodes            int = 8 //num of raft nodes
    numTerms            int = 10 //num of terms to loop through
    maxChanBuff         int = 10000 //max size of buffer
    minTimeOut          int = 150   //min timeout for trying to become leader
    maxTimeOut          int = 300 //max timeout for trying to become leader
    voteTimeOut         int64 = 100 //timeout for waiting for requested votes
    voteSentTimeOut     int64 = 250 //timeout waiting for vote to result in leader
    termDuration        int64 = 2000 //duration of term for leader for debugging
    leaderTimeOut       int64 = 150 //timeout for needed hb's from leader to followe
    
    //types of messages
    VOTE                int = 0 //request for a vote
    ACK                 int = 1 //ack for vote
    HB                  int = 2 //heartbeat of the leader
    LEADER              int = 3 //request to see if leader
    LEADER_LOG          int = 4 //client send log to leader
    LOG                 int = 5 //leader sending out log to commit
    LOG_ACK             int = 6 //follower ack'ing commit to log
)

type Message struct {
    messageType int
    sender      int
    newLog      Log
    prevLog     Log
}


func ElectLeader(peers []chan Message, id int, persister *Persister) bool {
    leaderSearching := true
    isLeader := false
    sentVote := false
    lastReset := time.Now()
    lastVote := time.Now()
    timeOut := int64(rand.Intn(maxTimeOut- minTimeOut) + minTimeOut)

    for leaderSearching { 
        if time.Since(lastReset).Milliseconds() < timeOut { //read messages in channel to see if others are requesting votes or are already the leader
            keepReading := true
            for keepReading {
                select {
                    case message := <- peers[id]:
                        if message.messageType == VOTE {
                            if !sentVote {
                                sentVote = true
                                peers[message.sender] <- Message{ACK, id, Log{}, Log{}}
                                lastVote = time.Now()
                                lastReset = time.Now()
                            }
                        } else if message.messageType == HB {
                            keepReading = false
                            leaderSearching = false
                        }
                        if time.Since(lastVote).Milliseconds() > voteSentTimeOut {
                            sentVote = false
                        }
                    default:
                        keepReading = false
                }
            }
        } else { //try to become the leader by gaining the majority of votes
            for i := 0; i < numNodes; i++ {
                if i != id {
                    peers[i] <- Message{VOTE, id, Log{}, Log{}}
                }
            }
            // need to gain majority before the timeout for voting occurs
            needed := numNodes / 2 + 1 //majority
            needed-- //vote for self
            voteStart := time.Now()
            
            for time.Since(voteStart).Milliseconds() < voteTimeOut {
                select {
                    case message := <- peers[id]:
                        if message.messageType == ACK {
                            needed--
                        } else if message.messageType == HB{ //another leader was already elected so can't be elected
                            needed = numNodes
                        }
                    default:
                        time.Sleep(10 * 1e6)
                }
            }
            
            if needed <= 0 { //elected
                leaderSearching = false
                isLeader = true
            } else { //not elected
                lastReset = time.Now()
                timeOut = int64(rand.Intn(maxTimeOut- minTimeOut) + minTimeOut)

            }
        }
        time.Sleep(10 * 1e6)
    }
    
    return isLeader
}

func RunLeader(peers []chan Message, applyChan chan Message, id int, ps *Persister, me *Follower, term int) {
    termStart := time.Now()
    leader := ps.MakeLeader(me, id)

    for time.Since(termStart).Milliseconds() < termDuration {
        for i := 0; i < numNodes; i++ {
            if i != id {
                peers[i] <- Message{HB, id, Log{}, Log{}}
            }
        }

        chanIsEmpty := false
        for !chanIsEmpty {
            select {
                case message := <- peers[id]:
                    switch message.messageType {
                        case LEADER: applyChan <- Message{ACK, id, Log{}, Log{}}
                        case LOG_ACK: ps.LeaderRecieveLogAck(leader, message)
                        case LEADER_LOG:
                            message.newLog.term = term
                            ps.FollowerRecieveLog(me, message.newLog, ps.logs[message.newLog.idx - 1])
                    }

                default:
                    chanIsEmpty = true
            }
        }

        ps.SendLogs(leader, peers, id)
        ps.UpdateLeaderLogCommits(leader, id)
        time.Sleep(1e6 * 100)
    }

    time.Sleep(1e6 * 100)
}


func RunFollower(peers []chan Message, id int, ps *Persister, me *Follower) {
    lastHB := time.Now()
    timeOut := int64(rand.Intn(maxTimeOut- minTimeOut) + minTimeOut)
    
    for time.Since(lastHB).Milliseconds() < timeOut {
        select {
            case message := <- peers[id]:
                switch message.messageType {
                    case HB: lastHB = time.Now()
                    case LOG: 
                        added := ps.FollowerRecieveLog(me, message.newLog, message.prevLog)
                        if added {
                            peers[message.sender] <- Message{LOG_ACK, id, message.newLog, message.prevLog}
                        }
                }

            default:
                time.Sleep(10 * 1e6)
        }
    }
}

func RaftNode(peers []chan Message, id int, ps *Persister, applyChan chan Message, mu sync.Mutex) {
    term := 0
    me := ps.MakeFollower()
    
    //loop for terms
    for term < 10 {
        isLeader := ElectLeader(peers, id, ps)
        
        if isLeader {
            fmt.Print("term ")
            fmt.Println(term)
            fmt.Println(id)
            RunLeader(peers, applyChan, id, ps, me, term)
        } else {
            RunFollower(peers, id, ps, me)
        }
        
        term++
        ps.PrintLogs(mu, id)
    }
    
}

func GetCurrentLeader(peers []chan Message, applyChan chan Message) int {
    leader := -1
    
    for i := 0; i < numNodes; i++ {
        peers[i] <- Message{LEADER, -1, Log{}, Log{}}
    }
    
    seacrhStart := time.Now()
    
    for leader < 0 && time.Since(seacrhStart).Milliseconds() < leaderTimeOut {
        select {
            case message := <- applyChan:
                if message.messageType == ACK {
                    leader = message.sender
                }
            default:
                time.Sleep(10 * 1e6)
        }
    }
    
    return leader
}

func main() {
    rand.Seed(time.Now().UnixNano())
    
    peers := make([]chan Message, numNodes)
    applyChan := make(chan Message, maxChanBuff)
    logs := make([]Log, 10)
    var printLock sync.Mutex
    
    for i := 0; i < numNodes; i++ {
        peers[i] = make(chan Message, maxChanBuff)
    }

    for i := 0; i < len(logs); i++ {
        logs[i] = MakeLog(-1, i+1, StateMachine{0, -1 * i, 2*i})
    }

    for i := 0; i < numNodes; i++ {
        persister := MakePersister()
        go RaftNode(peers, i, persister, applyChan, printLock)
    }

    time.Sleep(1e9)
    
    leader := -1
    for leader == -1 {
        leader = GetCurrentLeader(peers, applyChan)
    }
    
    peers[leader] <- Message{LEADER_LOG, -1, logs[0], Log{0, 0, true, StateMachine{0,0,0}}}


    for i := 1; i < len(logs); i++ {
        leader := -1
        for leader == -1 {
            leader = GetCurrentLeader(peers, applyChan)
        }

        peers[leader] <- Message{LEADER_LOG, -1, logs[i], logs[i-1]}

        time.Sleep(1.5e9)
    }
    
    time.Sleep(10e9)
}