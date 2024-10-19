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
	State() string
	setState(string)
	resetVote()
	setLeaderID(string)
	IsLeader() bool
	incrementElection(uint32)
	incrementAnswer(uint32)
	getNumElection() uint32
	getNumAnswer() uint32
	ToJSON(io.Writer, []byte) error
}

var (
	_ Node = (*VoterNode)(nil)
)

type NodeMeta struct {
	ID          string `json:"id"`
	Addr        string `json:"addr"`
	Port        int    `json:"port"`
	IsVoter     bool   `json:"isVoter"`
	State       string `json:"state"`
	Arbitrary   []byte `json:"arbitrary"`
	NumElection uint32 `json:"num-election"`
	NumAnswer   uint32 `json:"num-answer"`
	LeaderID    string `json:"leader-id"`
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
			id:          meta.ID,
			addr:        meta.Addr,
			port:        meta.Port,
			state:       meta.State,
			numElection: meta.NumElection,
			numAnswer:   meta.NumAnswer,
			leaderID:    meta.LeaderID,
		}
	}
	return &NonvoterNode{
		id:   meta.ID,
		addr: meta.Addr,
		port: meta.Port,
	}
}

type VoterNode struct {
	id          string
	addr        string
	port        int
	state       string
	numElection uint32
	numAnswer   uint32
	leaderID    string
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

func (vn *VoterNode) ToJSON(out io.Writer, arib []byte) error {
	return toJSON(out, NodeMeta{
		ID:          vn.id,
		Addr:        vn.addr,
		Port:        vn.port,
		IsVoter:     true,
		State:       vn.state,
		Arbitrary:   arib,
		NumElection: vn.numElection,
		NumAnswer:   vn.numAnswer,
		LeaderID:    vn.leaderID,
	})
}

func NewVoterNode(id string, addr string, port int, state string) *VoterNode {
	return &VoterNode{
		id:    id,
		addr:  addr,
		port:  port,
		state: state,
	}
}

var (
	_ Node = (*NonvoterNode)(nil)
)

type NonvoterNode struct {
	id   string
	addr string
	port int
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

func (nn *NonvoterNode) State() string {
	return "nonvoter"
}

func (nn *NonvoterNode) setState(string) {
	// nop
}

func (nn *NonvoterNode) resetVote() {
	// nop
}

func (nn *NonvoterNode) incrementElection(uint32) {
	// nop
}

func (nn *NonvoterNode) incrementAnswer(uint32) {
	// nop
}

func (nn *NonvoterNode) getNumElection() uint32 {
	return 0
}

func (nn *NonvoterNode) getNumAnswer() uint32 {
	return 0
}

func (nn *NonvoterNode) setLeaderID(string) {
	// nop
}

func (nn *NonvoterNode) IsLeader() bool {
	return false
}

func (nn *NonvoterNode) ToJSON(out io.Writer, arib []byte) error {
	return toJSON(out, NodeMeta{
		ID:          nn.id,
		Addr:        nn.addr,
		Port:        nn.port,
		IsVoter:     false,
		State:       "nonvoter",
		Arbitrary:   arib,
		NumElection: 0,
		NumAnswer:   0,
		LeaderID:    "",
	})
}

func NewNonvoterNode(id string, addr string, port int) *NonvoterNode {
	return &NonvoterNode{
		id:   id,
		addr: addr,
		port: port,
	}
}
