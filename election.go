package bullyelection

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrElectionTimeout         = errors.New("election timeout")
	ErrSyncCoordinator         = errors.New("sync coordinator")
	ErrBeginTransferLeadership = errors.New("transfer_leadership begging")
)

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

	if b.node.IsVoter() != true {
		b.opt.logger.Printf("debug: is not voter, skip election")
		return nil
	}

	b.opt.logger.Printf("debug: start election")
	defer b.opt.logger.Printf("debug: end election")

	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.syncState(stateWaitElection); err != nil {
		return errors.Wrapf(err, "state change : %s", stateElecting)
	}
	nodes, err := waitVoterNodes(ctx, b, stateWaitElection)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, n := range nodes {
		b.opt.logger.Printf("debug: current node %s is_leader=%v", n.ID(), n.IsLeader())
	}

	if len(nodes) < 2 {
		b.setLeaderID(b.node.ID())
		if err := b.updateNode(); err != nil {
			return errors.Wrapf(err, "promote self to leader timeout: %+v", err)
		}
		return nil
	}

	isNoLeader := true
	for _, n := range nodes {
		if n.IsLeader() {
			isNoLeader = false
		}
	}
	if isNoLeader {
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].getULID() < nodes[j].getULID()
		})
		temporaryLeaderID := nodes[0].ID()
		if temporaryLeaderID == b.node.ID() { // self
			b.setLeaderID(temporaryLeaderID)
		}
	}

	if b.node.IsLeader() {
		b.opt.logger.Printf("debug: leader synchronization")

		b.opt.logger.Printf("debug: leader wait follower ready msg: num=%d", len(nodes)-1)
		expectCollected := len(nodes) - 1 // without leader
		if err := waitCollected(ctx, b, expectCollected); err != nil {
			return errors.Wrapf(ErrElectionTimeout, "ready election timeout: %+v", errors.WithStack(err))
		}

		for _, n := range nodes {
			if err := b.sendReadySyncedMessage(n.ID()); err != nil {
				return errors.Wrapf(ErrElectionTimeout, "send ready synced: %+v", err)
			}
		}

		if err := b.syncState(stateElecting); err != nil {
			return errors.Wrapf(ErrElectionTimeout, "state change : %s", stateElecting)
		}
	} else {
		b.opt.logger.Printf("debug: follower synchronization")

		b.opt.logger.Printf("debug: follower send ready msg: num=%d", len(nodes)-1) // -1 = self
		for _, n := range nodes {
			b.opt.logger.Printf("debug: follower send ready to %s", n.ID())
			if err := b.sendReadyElectionMessage(n.ID()); err != nil {
				return errors.Wrapf(ErrSyncCoordinator, "case: %+v", errors.WithStack(err))
			}
		}

		b.opt.logger.Printf("debug: follower wait other node ready synched -> electing: %d", len(nodes))
		if _, err := waitVoterNodes(ctx, b, stateElecting); err != nil {
			return errors.WithStack(err)
		}
	}

	b.opt.logger.Printf("debug: nodes synchronized")

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

	if _, err := waitVoterNodes(ctx, b, stateRunning); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (b *Bully) startLeadershipTransfer(ctx context.Context) error {
	if b.node.IsLeader() != true {
		return nil
	}

	nodes, err := waitVoterNodes(ctx, b, stateRunning)
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

	if _, err := waitVoterNodes(ctx, b, stateRunning); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func waitCollected(ctx context.Context, b *Bully, expectCollected int) error {
	for {
		ch := make(chan int)
		b.recvReadyCount <- &recvReadyElectionCount{ch}

		select {
		case <-ctx.Done():
			return errors.WithStack(ErrSyncCoordinator)
		case count := <-ch:
			if expectCollected <= count {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-time.After(b.opt.electionInterval):
			// continue
		}
	}
}

func waitVoterNodes(ctx context.Context, b *Bully, targetState electionState) ([]internalVoterNode, error) {
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

func isAllState(nodes []internalVoterNode, targetState electionState) bool {
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
		if m.IsVoter() {
			voters = append(voters, m.(internalVoterNode))
		}
	}
	return voters
}
