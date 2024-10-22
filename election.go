package bullyelection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrElectionTimeout         = errors.New("election timeout")
	ErrSyncCoordinator         = errors.New("sync coordinator")
	ErrBeginTransferLeadership = errors.New("transfer_leadership begging")
)

type ElectionState string

const (
	StateInitial            ElectionState = "init"
	StateRunning            ElectionState = "running"
	StateElecting           ElectionState = "electing"
	StateTransferLeadership ElectionState = "transfer_leader"
)

func (s ElectionState) String() string {
	return string(s)
}

type Message struct {
	Type   NodeMessageType `json:"type"`
	NodeID string          `json:"node-id"`
}

func marshalNodeMessage(out io.Writer, msgType NodeMessageType, nodeID string) error {
	if err := json.NewEncoder(out).Encode(Message{msgType, nodeID}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func unmarshalNodeMessage(in io.Reader) (Message, error) {
	msg := Message{}
	if err := json.NewDecoder(in).Decode(&msg); err != nil {
		return Message{}, errors.WithStack(err)
	}
	return msg, nil
}

func (b *Bully) startElection(ctx context.Context) (err error) {
	defer func() {
		if b.waitElection != nil {
			select {
			case b.waitElection <- err:
				// ok
			default:
				b.opt.logger.Printf("warn: no reader: wait election, drop")
			}
		}
	}()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.node.IsVoterNode() != true {
		return nil
	}

	nodes, err := waitVoterNodes(ctx, b, StateElecting)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(nodes) < 1 {
		return nil
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].getULID() < nodes[j].getULID()
	})

	candidateLeader := nodes[0]
	if candidateLeader.ID() == b.node.ID() {
		for _, n := range nodes {
			if err := b.sendCoordinatorMessage(n.ID()); err != nil {
				return errors.Wrapf(ErrSyncCoordinator, "case: %+v", errors.WithStack(err))
			}
		}
	}

	if _, err := waitVoterNodes(ctx, b, StateRunning); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) startLeadershipTransfer(ctx context.Context) error {
	if b.node.IsLeader() != true {
		return nil
	}

	nodes, err := waitVoterNodes(ctx, b, StateRunning)
	if err != nil {
		return errors.Wrapf(ErrBeginTransferLeadership, "all not running state: %+v", errors.WithStack(err))
	}

	if len(nodes) < 1 {
		return nil
	}

	b.setULID(b.opt.ulidGeneratorFunc())
	if err := b.updateNode(); err != nil {
		return errors.Wrapf(ErrBeginTransferLeadership, "update ulid: %+v", errors.WithStack(err))
	}

	for _, n := range nodes {
		if err := b.sendTransferLeaderMessage(n.ID()); err != nil {
			return errors.Wrapf(ErrSyncCoordinator, "case: %+v", errors.WithStack(err))
		}
	}

	if _, err := waitVoterNodes(ctx, b, StateRunning); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func waitVoterNodes(ctx context.Context, b *Bully, targetState ElectionState) ([]internalVoterNode, error) {
	for {
		voters := filterVoterNodes(b.listNodes())
		if isAllState(voters, targetState) {
			return voters, nil
		}

		select {
		case <-ctx.Done():
			msg := bytes.NewBuffer(nil)
			msg.WriteString("[")
			for _, n := range voters {
				fmt.Fprintf(msg, "id:%s state:%s", n.ID(), n.getState())
			}
			msg.WriteString("]")
			return nil, errors.Wrapf(ErrElectionTimeout, msg.String())
		case <-time.After(b.opt.electionInterval):
			// continue
		}
	}
}

func isAllState(nodes []internalVoterNode, targetState ElectionState) bool {
	for _, node := range nodes {
		if node.getState() != targetState.String() {
			return false
		}
	}
	return true
}

func filterVoterNodes(members []Node) []internalVoterNode {
	voters := make([]internalVoterNode, 0, len(members))
	for _, m := range members {
		if m.IsVoterNode() {
			voters = append(voters, m.(internalVoterNode))
		}
	}
	return voters
}
