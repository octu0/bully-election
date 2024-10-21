package bullyelection

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

type Node interface {
	ID() string
	Addr() string
	Port() int
	IsVoterNode() bool

	// leader
	IsLeader() bool

	// user metadata
	UserMetadata() []byte
	setUserMetadata([]byte)

	// node meta
	toJSON(io.Writer) error
}

type internalVoterNode interface {
	Node

	// state
	State() string
	setState(string)

	// vote
	resetVote()
	incrementElection(uint32)
	incrementAnswer(uint32)
	getNumElection() uint32
	getNumAnswer() uint32

	setLeaderID(string)

	getNodeNumber() int64
	setNodeNumber(int64)
}

var (
	_ Node              = (*voterNode)(nil)
	_ internalVoterNode = (*voterNode)(nil)
)

type nodeMeta struct {
	ID           string `json:"id"`
	Addr         string `json:"addr"`
	Port         int    `json:"port"`
	IsVoter      bool   `json:"isVoter"`
	State        string `json:"state"`
	UserMetadata []byte `json:"user-metadata"`
	NumElection  uint32 `json:"num-election"`
	NumAnswer    uint32 `json:"num-answer"`
	LeaderID     string `json:"leader-id"`
	NodeNumber   int64  `json:"node-number"`
}

func toJSON(out io.Writer, meta nodeMeta) error {
	if err := json.NewEncoder(out).Encode(meta); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func fromJSON(in io.Reader) (nodeMeta, error) {
	meta := nodeMeta{}
	if err := json.NewDecoder(in).Decode(&meta); err != nil {
		return nodeMeta{}, errors.WithStack(err)
	}
	return meta, nil
}

func nodeMetaToNode(meta nodeMeta) Node {
	if meta.IsVoter {
		return &voterNode{
			id:           meta.ID,
			addr:         meta.Addr,
			port:         meta.Port,
			state:        meta.State,
			numElection:  meta.NumElection,
			numAnswer:    meta.NumAnswer,
			leaderID:     meta.LeaderID,
			userMetadata: meta.UserMetadata,
			nodeNumber:   meta.NodeNumber,
		}
	}
	return &nonvoterNode{
		id:           meta.ID,
		addr:         meta.Addr,
		port:         meta.Port,
		userMetadata: meta.UserMetadata,
	}
}

type voterNode struct {
	id           string
	addr         string
	port         int
	state        string
	numElection  uint32
	numAnswer    uint32
	leaderID     string
	userMetadata []byte
	nodeNumber   int64
}

func (vn *voterNode) ID() string {
	return vn.id
}

func (vn *voterNode) Addr() string {
	return vn.addr
}

func (vn *voterNode) Port() int {
	return vn.port
}

func (vn *voterNode) IsVoterNode() bool {
	return true
}

func (vn *voterNode) State() string {
	return vn.state
}

func (vn *voterNode) UserMetadata() []byte {
	return vn.userMetadata
}

func (vn *voterNode) setUserMetadata(data []byte) {
	vn.userMetadata = data
}

func (vn *voterNode) setState(newState string) {
	vn.state = newState
}

func (vn *voterNode) resetVote() {
	vn.numElection = 0
	vn.numAnswer = 0
}

func (vn *voterNode) incrementElection(n uint32) {
	vn.numElection += n
}

func (vn *voterNode) incrementAnswer(n uint32) {
	vn.numAnswer += n
}

func (vn *voterNode) getNumElection() uint32 {
	return vn.numElection
}

func (vn *voterNode) getNumAnswer() uint32 {
	return vn.numAnswer
}

func (vn *voterNode) setLeaderID(id string) {
	vn.leaderID = id
}

func (vn *voterNode) IsLeader() bool {
	return vn.id == vn.leaderID
}

func (vn *voterNode) setNodeNumber(num int64) {
	vn.nodeNumber = num
}

func (vn *voterNode) getNodeNumber() int64 {
	return vn.nodeNumber
}

func (vn *voterNode) toJSON(out io.Writer) error {
	return toJSON(out, nodeMeta{
		ID:           vn.id,
		Addr:         vn.addr,
		Port:         vn.port,
		IsVoter:      true,
		State:        vn.state,
		UserMetadata: vn.userMetadata,
		NumElection:  vn.numElection,
		NumAnswer:    vn.numAnswer,
		LeaderID:     vn.leaderID,
		NodeNumber:   vn.nodeNumber,
	})
}

func newVoterNode(id string, addr string, port int, state string, nodeNum int64) *voterNode {
	return &voterNode{
		id:         id,
		addr:       addr,
		port:       port,
		state:      state,
		nodeNumber: nodeNum,
	}
}

var (
	_ Node = (*nonvoterNode)(nil)
)

type nonvoterNode struct {
	id           string
	addr         string
	port         int
	userMetadata []byte
}

func (nn *nonvoterNode) ID() string {
	return nn.id
}

func (nn *nonvoterNode) Addr() string {
	return nn.addr
}

func (nn *nonvoterNode) Port() int {
	return nn.port
}

func (nn *nonvoterNode) IsVoterNode() bool {
	return false
}

func (nn *nonvoterNode) IsLeader() bool {
	return false
}

func (nn *nonvoterNode) UserMetadata() []byte {
	return nn.userMetadata
}

func (nn *nonvoterNode) setUserMetadata(data []byte) {
	nn.userMetadata = data
}

func (nn *nonvoterNode) toJSON(out io.Writer) error {
	return toJSON(out, nodeMeta{
		ID:           nn.id,
		Addr:         nn.addr,
		Port:         nn.port,
		IsVoter:      false,
		State:        "",
		UserMetadata: nn.userMetadata,
		NodeNumber:   -1,
	})
}

func newNonvoterNode(id string, addr string, port int) *nonvoterNode {
	return &nonvoterNode{
		id:   id,
		addr: addr,
		port: port,
	}
}
