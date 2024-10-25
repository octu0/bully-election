package bullyelection

import "sync"

type electionState string

const (
	stateInitial            electionState = "init"
	stateRunning            electionState = "running"
	stateWaitElection       electionState = "wait_election"
	stateElecting           electionState = "electing"
	stateTransferLeadership electionState = "transfer_leader"
)

func (s electionState) String() string {
	return string(s)
}

type stateMachine struct {
	sync.RWMutex

	electedNodes  []string
	answeredNodes []string
	leaderID      string
}

func (stm *stateMachine) AddElected(nodeID string) {
	stm.Lock()
	defer stm.Unlock()

	stm.electedNodes = append(stm.electedNodes, nodeID)
}

func (stm *stateMachine) AddAnswered(nodeID string) {
	stm.Lock()
	defer stm.Unlock()

	stm.answeredNodes = append(stm.answeredNodes, nodeID)
}

func (stm *stateMachine) SetLeaderID(nodeID string) {
	stm.Lock()
	defer stm.Unlock()

	stm.leaderID = nodeID
}

func (stm *stateMachine) Reset() {
	stm.Lock()
	defer stm.Unlock()

	stm.electedNodes = make([]string, 0)
	stm.answeredNodes = make([]string, 0)
	stm.leaderID = ""
}

func newStateMachine() *stateMachine {
	stm := &stateMachine{}
	stm.Reset()
	return stm
}
