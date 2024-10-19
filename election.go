package bullyelection

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrElectionTimeout = errors.New("election timeout")
	ErrElectionCancel  = errors.New("election cancel")
	ErrTooManyLeader   = errors.New("too many leader")
	ErrNoLeader        = errors.New("no leader")
)

func startElection(ctx context.Context, b *Bully) error {
	if b.IsVoter() != true {
		return nil
	}
	timeout := 30 * time.Second
	interval := 100 * time.Millisecond

	b.setState(StateElecting)
	b.resetVote()
	if err := b.updateNode(10 * time.Second); err != nil {
		return errors.WithStack(err)
	}

	// wait state change
	voterNodes, err := waitVoterNodes(ctx, b, StateElecting, timeout, interval)
	if err != nil {
		return errors.WithStack(err)
	}

	// selects targets smaller than its own ID
	targetNodes := make([]Node, 0, len(voterNodes))
	for _, n := range voterNodes {
		if n.ID() < b.ID() {
			targetNodes = append(targetNodes, n)
		}
	}

	for _, n := range targetNodes {
		if err := b.sendElection(n.ID()); err != nil {
			return errors.WithStack(err)
		}
	}

	expectNumAnsweredNodes := len(voterNodes) - 1
	nodes, err := waitNodeStateAnswered(ctx, b, expectNumAnsweredNodes, timeout, interval)
	if err != nil {
		return errors.WithStack(err)
	}

	leaderNodes := make([]Node, 0, 1)
	for _, n := range nodes {
		if n.getNumElection() == 0 {
			leaderNodes = append(leaderNodes, n)
		}
	}

	// must one node
	if 1 < len(leaderNodes) {
		return errors.Wrapf(ErrTooManyLeader, "nodes = %+v", leaderNodes)
	}
	if len(leaderNodes) == 0 {
		msg := bytes.NewBuffer(nil)
		for _, n := range nodes {
			msg.WriteString(fmt.Sprintf("id:%s election:%d answer:%d\n", n.ID(), n.getNumElection(), n.getNumAnswer()))
		}
		return errors.Wrapf(ErrNoLeader, msg.String())
	}
	leaderNode := leaderNodes[0]
	if leaderNode.ID() == b.ID() {
		for _, n := range nodes {
			if err := b.sendCoordinator(n.ID()); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	if _, err := waitVoterNodes(ctx, b, StateElected, timeout, interval); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func waitNodeStateAnswered(ctx context.Context, b *Bully, expectNum int, timeout, interval time.Duration) ([]Node, error) {
	deadline := time.Now().Add(timeout)
	for {
		if deadline.UnixNano() < time.Now().UnixNano() {
			return nil, errors.WithStack(ErrElectionTimeout)
		}

		voters := getVoterNodes(b)
		num := numState(voters, StateAnswered)
		if expectNum == num {
			return voters, nil
		}

		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ErrElectionCancel)
		case <-time.After(interval):
			// continue
		}
	}
}

func waitVoterNodes(ctx context.Context, b *Bully, targetState State, timeout, interval time.Duration) ([]Node, error) {
	deadline := time.Now().Add(timeout)
	for {
		if deadline.UnixNano() < time.Now().UnixNano() {
			return nil, errors.WithStack(ErrElectionTimeout)
		}

		voters := getVoterNodes(b)
		if isAllState(voters, targetState) {
			return voters, nil
		}

		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ErrElectionCancel)
		case <-time.After(interval):
			// continue
		}
	}
}

func isAllState(nodes []Node, targetState State) bool {
	for _, node := range nodes {
		if node.State() != targetState.String() {
			return false
		}
	}
	return true
}

func numState(nodes []Node, targetState State) int {
	total := 0
	for _, node := range nodes {
		if node.State() == targetState.String() {
			total += 1
		}
	}
	return total
}

func getVoterNodes(b *Bully) []Node {
	members := b.Members()
	voters := make([]Node, 0, len(members))
	for _, m := range members {
		if m.IsVoterNode() {
			voters = append(voters, m)
		}
	}
	return voters
}
