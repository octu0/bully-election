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
	setPort(int)

	// node type
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
	getState() string
	setState(string)

	setLeaderID(string)

	getULID() string
	setULID(string)
}

var (
	_ Node              = (*voterNode)(nil)
	_ internalVoterNode = (*voterNode)(nil)
)

type nodeMeta struct {
	ID           string `json:"id"`
	ULID         string `json:"ulid"`
	Addr         string `json:"addr"`
	Port         int    `json:"port"`
	IsVoter      bool   `json:"isVoter"`
	State        string `json:"state"`
	UserMetadata []byte `json:"user-metadata"`
	LeaderID     string `json:"leader-id"`
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
			leaderID:     meta.LeaderID,
			userMetadata: meta.UserMetadata,
			ulid:         meta.ULID,
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
	ulid         string
	addr         string
	port         int
	state        string
	leaderID     string
	userMetadata []byte
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

func (vn *voterNode) setPort(p int) {
	vn.port = p
}

func (vn *voterNode) IsVoterNode() bool {
	return true
}

func (vn *voterNode) getState() string {
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

func (vn *voterNode) setLeaderID(id string) {
	vn.leaderID = id
}

func (vn *voterNode) IsLeader() bool {
	return vn.id == vn.leaderID
}

func (vn *voterNode) getULID() string {
	return vn.ulid
}

func (vn *voterNode) setULID(ulid string) {
	vn.ulid = ulid
}

func (vn *voterNode) toJSON(out io.Writer) error {
	return toJSON(out, nodeMeta{
		ID:           vn.id,
		Addr:         vn.addr,
		Port:         vn.port,
		IsVoter:      true,
		State:        vn.state,
		UserMetadata: vn.userMetadata,
		LeaderID:     vn.leaderID,
		ULID:         vn.ulid,
	})
}

func newVoterNode(id string, ulid string, addr string, port int, state string) *voterNode {
	return &voterNode{
		id:       id,
		ulid:     ulid,
		addr:     addr,
		port:     port,
		state:    state,
		leaderID: id, // initial self
	}
}

var (
	_ Node = (*nonvoterNode)(nil)
)

type nonvoterNode struct {
	id           string
	ulid         string
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

func (nn *nonvoterNode) setPort(p int) {
	nn.port = p
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
		ULID:         nn.ulid,
		Addr:         nn.addr,
		Port:         nn.port,
		IsVoter:      false,
		State:        "",
		UserMetadata: nn.userMetadata,
	})
}

func newNonvoterNode(id string, ulid string, addr string, port int) *nonvoterNode {
	return &nonvoterNode{
		id:   id,
		ulid: ulid,
		addr: addr,
		port: port,
	}
}
