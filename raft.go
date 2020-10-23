package main

import (
	"fmt"
//	"sync"
	"math/rand"
	"time"
)

const (
	numNodes 				int = 8 //num of raft nodes
	maxChanBuff 		int = 10000 //max size of buffer
	minTimeOut 			int = 150	//min timeout for trying to become leader
	maxTimeOut 			int = 300 //max timeout for trying to become leader
	voteTimeOut 		int64 = 100 //timeout for waiting for requested votes
	voteSentTimeOut int64 = 250 //timeout waiting for vote to result in leader
	termDuration 		int64 = 500 //duration of term for leader for debugging
	leaderTimeOut 	int64 = 150 //timeout for needed hb's from leader to follower

	//types of messages
	VOTE		int = 0 //request for a vote
	ACK			int = 1	//ack for vote
	HB			int = 2	//heartbeat of the leader
	LEADER	int = 3	//request to see if leader
)

type Message struct {
	messageType int
	sender			int
}

func ElectLeader(peers []chan Message, id int, persister *Persister) bool {
	leaderSearching := true
	isLeader := false
	sentVote := false
	lastReset := time.Now()
	lastVote := time.Now()
	timeOut := int64(rand.Intn(maxTimeOut- minTimeOut) + minTimeOut)

	for leaderSearching { 

		//try to become the leader by gaining the majority of votes
		if time.Since(lastReset).Milliseconds() > timeOut {
			for i := 0; i < numNodes; i++ {
				if i != id {
					peers[i] <- Message{VOTE, id}
				}
			}

			// need to gain majority before the timeout for voting occurs
			needed := numNodes / 2 + 1 //majority
			needed-- //vote for self
			voteStart := time.Now()
			for needed != 0 && time.Since(voteStart).Milliseconds() < voteTimeOut {
				select {
					case message := <- peers[id]:
						if message.messageType == ACK {
							needed--
						}
					default:
						time.Sleep(10 * 1e6)
				}
			}
			if needed == 0 { //elected
				leaderSearching = false
				isLeader = true
			} else { //not elected
				lastReset = time.Now()
				timeOut = int64(rand.Intn(maxTimeOut- minTimeOut) + minTimeOut)

			}

		//read messages in channel to see if others are requesting votes or are already the leader
		} else { 
			keepReading := true
			for keepReading {
				select {
					case message := <- peers[id]:
						if message.messageType == VOTE {
							if !sentVote {
								sentVote = true
								peers[message.sender] <- Message{ACK, id}
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
		}
		time.Sleep(10 * 1e6)
	}
	return isLeader
}

func RunLeader(peers []chan Message, id int, persister *Persister, applyChan chan Message) {
	termStart := time.Now()
	for time.Since(termStart).Milliseconds() < termDuration {
		for i := 0; i < numNodes; i++ {
			if i != id {
				peers[i] <- Message{HB, id}
			}
		}
		select{
		case message := <- peers[id]:
			if message.messageType == LEADER {
				applyChan <- Message{ACK, id}
			}
		}
		time.Sleep(1e6 * 100)
	}
	time.Sleep(1e6 * 100)
}


func RunFollower(peers []chan Message, id int, persister *Persister) {
	lastHB := time.Now()
	for time.Since(lastHB).Milliseconds() < leaderTimeOut {
		select {
			case message := <- peers[id]:
				if message.messageType == HB {
					lastHB = time.Now()
				}
			default:
				time.Sleep(10 * 1e6)
		}
	}
}

func RaftNode(peers []chan Message, id int, persister *Persister, applyChan chan Message) {
	term := 0
	//loop forever
	for term < 10 {
		isLeader := ElectLeader(peers, id, persister)
		if isLeader {
			fmt.Print("term ")
			fmt.Println(term)
			fmt.Println(id)
			RunLeader(peers, id, persister, applyChan)
		} else {
			RunFollower(peers, id, persister)
		}
		term++
	}
	
}

func GetCurrentLeader(peers []chan Message, applyChan chan Message) int {
	leader := -1
	for i := 0; i < numNodes; i++ {
		peers[i] <- Message{LEADER, -1}
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
	//make channels between the nodes
	rand.Seed(time.Now().UnixNano())
	peers := make([]chan Message, numNodes)
	applyChan := make(chan Message, maxChanBuff)
	persister := MakePersister()
	for i := 0; i < numNodes; i++ {
		peers[i] = make(chan Message, maxChanBuff)
	}

	for i := 0; i < numNodes; i++ {
		go RaftNode(peers, i, persister, applyChan)
	}
/*
	for i := 0; i < 5; i++ {
		fmt.Print("Current Leader: ")
		fmt.Println(GetCurrentLeader(peers, applyChan))
		time.Sleep(1e9)
	}
	*/
	time.Sleep(10e9)
}