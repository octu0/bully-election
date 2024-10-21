package bullyelection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrElectionTimeout       = errors.New("election timeout")
	ErrTooManyLeader         = errors.New("too many leader")
	ErrNoLeader              = errors.New("no leader")
	ErrSyncCoordinator       = errors.New("sync coordinator")
	ErrNodeNotReadyCondition = errors.New("node not ready condition")
	ErrBeginTransferLeader   = errors.New("begin transfer leader")
)

type State string

const (
	StateInitial        State = "init"
	StateRunning        State = "running"
	StateElecting       State = "electing"
	StateAnswered       State = "answered"
	StateTransferLeader State = "transfer-leader"
)

func (s State) String() string {
	return string(s)
}

type MessageType uint8

const (
	ElectionMessage MessageType = iota + 1
	AnswerMessage
	CoordinatorMessage
	TransferLeaderMessage
)

type Message struct {
	Type   MessageType `json:"type"`
	NodeID string      `json:"node-id"`
}

func marshalMessage(out io.Writer, msgType MessageType, nodeID string) error {
	if err := json.NewEncoder(out).Encode(Message{msgType, nodeID}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func unmarshalMessage(in io.Reader) (Message, error) {
	msg := Message{}
	if err := json.NewDecoder(in).Decode(&msg); err != nil {
		return Message{}, errors.WithStack(err)
	}
	return msg, nil
}

func startElection(parent context.Context, b *Bully) error {
	if b.IsVoter() != true {
		return nil
	}

	ctx, cancel := context.WithTimeout(parent, b.opt.electionTimeout)
	defer cancel()

	b.setState(StateElecting)
	b.resetVote()
	if err := b.updateNode(); err != nil {
		return errors.WithStack(err)
	}

	// wait state change
	voterNodes, err := waitVoterNodes(ctx, b, StateElecting)
	if err != nil {
		return errors.WithStack(err)
	}

	// no voter = lat node
	if len(voterNodes) < 1 {
		b.setState(StateRunning)
		if err := b.updateNode(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	if err := leaderElection(ctx, b, voterNodes); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func startTransferLeader(parent context.Context, b *Bully) error {
	if b.IsVoter() != true {
		return nil
	}
	if b.IsLeader() != true {
		return nil
	}

	ctx, cancel := context.WithTimeout(parent, b.opt.electionTimeout)
	defer cancel()

	// all nodes must state = running
	nodes, err := waitVoterNodes(ctx, b, StateRunning)
	if err != nil {
		return errors.Wrapf(ErrNodeNotReadyCondition, "cause: %+v", err)
	}

	b.setNodeNumber(b.opt.nodeNumberGenFunc(b.ID()))
	if err := b.updateNode(); err != nil {
		return errors.Wrapf(ErrBeginTransferLeader, "cause: %+v", err)
	}

	for _, n := range nodes {
		if err := b.sendTransferLeader(n.ID()); err != nil {
			return errors.Wrapf(ErrBeginTransferLeader, "cause: %+v", err)
		}
	}

	// wait state change
	voterNodes, err := waitVoterNodes(ctx, b, StateTransferLeader)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := leaderElection(ctx, b, voterNodes); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func leaderElection(ctx context.Context, b *Bully, voterNodes []internalVoterNode) error {
	// selects targets smaller than its own NodeNumber
	selfNodeNumber := b.getNodeNumber()
	targetNodes := make([]internalVoterNode, 0, len(voterNodes))
	for _, n := range voterNodes {
		if n.getNodeNumber() < selfNodeNumber {
			targetNodes = append(targetNodes, n)
		}
	}

	for _, n := range targetNodes {
		if err := b.sendElection(n.ID()); err != nil {
			return errors.WithStack(err)
		}
	}

	expectNumAnsweredNodes := len(voterNodes) - 1
	nodes, err := waitNodeStateAnswered(ctx, b, expectNumAnsweredNodes)
	if err != nil {
		return errors.WithStack(err)
	}

	if _, err := syncLeader(ctx, b, nodes); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func syncLeader(ctx context.Context, b *Bully, answerNodes []internalVoterNode) (internalVoterNode, error) {
	leaderNodes := make([]internalVoterNode, 0, 1)
	for _, n := range answerNodes {
		if n.getNumElection() == 0 {
			leaderNodes = append(leaderNodes, n)
		}
	}

	// must one node
	if 1 < len(leaderNodes) {
		return nil, errors.Wrapf(ErrTooManyLeader, "nodes = %+v", leaderNodes)
	}

	if len(leaderNodes) == 0 {
		msg := bytes.NewBuffer(nil)
		for _, n := range answerNodes {
			msg.WriteString(fmt.Sprintf("id:%s election:%d answer:%d\n", n.ID(), n.getNumElection(), n.getNumAnswer()))
		}
		return nil, errors.Wrapf(ErrNoLeader, msg.String())
	}

	leaderNode := leaderNodes[0]
	if leaderNode.ID() == b.ID() { // leader only
		for _, n := range answerNodes {
			if err := b.sendCoordinator(n.ID()); err != nil {
				return nil, errors.Wrapf(ErrSyncCoordinator, "cause:%+v", err)
			}
		}
	}

	if _, err := waitVoterNodes(ctx, b, StateRunning); err != nil {
		return nil, errors.WithStack(err)
	}

	return leaderNode, nil
}

func handleMessage(_ context.Context, b *Bully, msg Message) error {
	if b.IsVoter() != true {
		return nil
	}

	switch msg.Type {
	case ElectionMessage:
		if err := b.sendAnswer(msg.NodeID); err != nil {
			return errors.Wrapf(err, "send answer(%s)", msg.NodeID)
		}
		b.incrementElection(1)
		b.setState(StateAnswered)
		if err := b.updateNode(); err != nil {
			return errors.Wrapf(err, "updateNode timeout")
		}

	case AnswerMessage:
		b.incrementAnswer(1)
		if err := b.updateNode(); err != nil {
			return errors.Wrapf(err, "updateNode timeout")
		}

	case CoordinatorMessage:
		b.setLeaderID(msg.NodeID)
		b.setState(StateRunning)
		if err := b.updateNode(); err != nil {
			return errors.Wrapf(err, "updateNode timeout")
		}

	case TransferLeaderMessage:
		b.setState(StateTransferLeader)
		if err := b.updateNode(); err != nil {
			return errors.Wrapf(err, "updateNode timeout")
		}
	}
	return nil
}

func waitNodeStateAnswered(ctx context.Context, b *Bully, expectNum int) ([]internalVoterNode, error) {
	for {
		voters := getVoterNodes(b)

		num := numState(voters, StateAnswered)
		if expectNum == num {
			return voters, nil
		}

		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ErrElectionTimeout)
		case <-time.After(b.opt.electionInterval):
			// continue
		}
	}
}

func waitVoterNodes(ctx context.Context, b *Bully, targetState State) ([]internalVoterNode, error) {
	for {
		voters := getVoterNodes(b)
		if isAllState(voters, targetState) {
			return voters, nil
		}

		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ErrElectionTimeout)
		case <-time.After(b.opt.electionInterval):
			// continue
		}
	}
}

func isAllState(nodes []internalVoterNode, targetState State) bool {
	for _, node := range nodes {
		if node.State() != targetState.String() {
			return false
		}
	}
	return true
}

func numState(nodes []internalVoterNode, targetState State) int {
	total := 0
	for _, node := range nodes {
		if node.State() == targetState.String() {
			total += 1
		}
	}
	return total
}

func getVoterNodes(b *Bully) []internalVoterNode {
	members := b.Members()
	voters := make([]internalVoterNode, 0, len(members))
	for _, m := range members {
		if m.IsVoterNode() {
			voters = append(voters, m.(internalVoterNode))
		}
	}
	return voters
}
