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
	_ Node              = (*VoterNode)(nil)
	_ internalVoterNode = (*VoterNode)(nil)
)

type NodeMeta struct {
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

func toJSON(out io.Writer, meta NodeMeta) error {
	if err := json.NewEncoder(out).Encode(meta); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func fromJSON(in io.Reader) (NodeMeta, error) {
	meta := NodeMeta{}
	if err := json.NewDecoder(in).Decode(&meta); err != nil {
		return NodeMeta{}, errors.WithStack(err)
	}
	return meta, nil
}

func nodeMetaToNode(meta NodeMeta) Node {
	if meta.IsVoter {
		return &VoterNode{
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
	return &NonvoterNode{
		id:           meta.ID,
		addr:         meta.Addr,
		port:         meta.Port,
		userMetadata: meta.UserMetadata,
	}
}

type VoterNode struct {
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

func (vn *VoterNode) ID() string {
	return vn.id
}

func (vn *VoterNode) Addr() string {
	return vn.addr
}

func (vn *VoterNode) Port() int {
	return vn.port
}

func (vn *VoterNode) IsVoterNode() bool {
	return true
}

func (vn *VoterNode) State() string {
	return vn.state
}

func (vn *VoterNode) UserMetadata() []byte {
	return vn.userMetadata
}

func (vn *VoterNode) setUserMetadata(data []byte) {
	vn.userMetadata = data
}

func (vn *VoterNode) setState(newState string) {
	vn.state = newState
}

func (vn *VoterNode) resetVote() {
	vn.numElection = 0
	vn.numAnswer = 0
}

func (vn *VoterNode) incrementElection(n uint32) {
	vn.numElection += n
}

func (vn *VoterNode) incrementAnswer(n uint32) {
	vn.numAnswer += n
}

func (vn *VoterNode) getNumElection() uint32 {
	return vn.numElection
}

func (vn *VoterNode) getNumAnswer() uint32 {
	return vn.numAnswer
}

func (vn *VoterNode) setLeaderID(id string) {
	vn.leaderID = id
}

func (vn *VoterNode) IsLeader() bool {
	return vn.id == vn.leaderID
}

func (vn *VoterNode) setNodeNumber(num int64) {
	vn.nodeNumber = num
}

func (vn *VoterNode) getNodeNumber() int64 {
	return vn.nodeNumber
}

func (vn *VoterNode) toJSON(out io.Writer) error {
	return toJSON(out, NodeMeta{
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

func NewVoterNode(id string, addr string, port int, state string, nodeNum int64) *VoterNode {
	return &VoterNode{
		id:         id,
		addr:       addr,
		port:       port,
		state:      state,
		nodeNumber: nodeNum,
	}
}

var (
	_ Node = (*NonvoterNode)(nil)
)

type NonvoterNode struct {
	id           string
	addr         string
	port         int
	userMetadata []byte
}

func (nn *NonvoterNode) ID() string {
	return nn.id
}

func (nn *NonvoterNode) Addr() string {
	return nn.addr
}

func (nn *NonvoterNode) Port() int {
	return nn.port
}

func (nn *NonvoterNode) IsVoterNode() bool {
	return false
}

func (nn *NonvoterNode) IsLeader() bool {
	return false
}

func (nn *NonvoterNode) UserMetadata() []byte {
	return nn.userMetadata
}

func (nn *NonvoterNode) setUserMetadata(data []byte) {
	nn.userMetadata = data
}

func (nn *NonvoterNode) toJSON(out io.Writer) error {
	return toJSON(out, NodeMeta{
		ID:           nn.id,
		Addr:         nn.addr,
		Port:         nn.port,
		IsVoter:      false,
		State:        "",
		UserMetadata: nn.userMetadata,
		NodeNumber:   -1,
	})
}

func NewNonvoterNode(id string, addr string, port int) *NonvoterNode {
	return &NonvoterNode{
		id:   id,
		addr: addr,
		port: port,
	}
}
