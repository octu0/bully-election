package bullyelection

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrElection           = errors.New("failed to election")
	ErrTransferLeadership = errors.New("failed  to transfer_leadership")
)

func (b *Bully) startElection(ctx context.Context, event NodeEvent, nodeID string) error {
	if b.node.IsVoter() != true {
		b.opt.logger.Printf("debug: is not voter, skip election")
		return nil
	}
	if event == LeaveEvent && b.node.ID() == nodeID {
		return nil // leave self
	}

	b.opt.logger.Printf("debug: start election")
	defer b.opt.logger.Printf("debug: end election")

	b.mu.Lock()
	defer b.mu.Unlock()

	b.electionCancel()
	b.electionCancel = nopCancelFunc()

	voterNodes := getVoters(b)
	if len(voterNodes) < 2 { // promote self
		for _, n := range b.listNodes() {
			if err := b.sendCoordinatorMessage(n.ID()); err != nil {
				b.opt.onErrorFunc(errors.Wrapf(ErrElection, "send coordinator: %+v", err))
			}
		}
		return nil
	}

	selfULID := b.getULID()
	for _, n := range voterNodes {
		// electable = smaller than own ULID
		if n.getULID() < selfULID {
			if err := b.sendElectionMessage(n.ID()); err != nil {
				return errors.Wrapf(ErrElection, "send election: %+v", err)
			}
		}
	}
	electionCtx, electionCancel := context.WithTimeout(ctx, b.opt.electionTimeout*2) // electionTimeout^2 = max timeout
	go func() {
		select {
		case <-electionCtx.Done():
			return

		case <-time.After(b.opt.electionTimeout):
			// send all nodes
			for _, n := range b.listNodes() {
				if err := b.sendCoordinatorMessage(n.ID()); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(ErrElection, "send coordinator: %+v", err))
				}
			}
		}
	}()
	b.electionCancel = electionCancel

	return nil
}

func (b *Bully) startLeadershipTransfer(ctx context.Context) error {
	if b.node.IsLeader() != true {
		return nil
	}

	nodes := getVoters(b)
	if len(nodes) < 1 {
		return nil
	}

	b.setULID(b.opt.ulidGeneratorFunc())
	if err := b.updateNode(); err != nil {
		return errors.Wrapf(ErrTransferLeadership, "update ulid: %+v", errors.WithStack(err))
	}

	for _, n := range nodes {
		if err := b.sendTransferLeaderMessage(n.ID()); err != nil {
			return errors.Wrapf(ErrTransferLeadership, "case: %+v", errors.WithStack(err))
		}
	}

	return nil
}

func getVoters(b *Bully) []internalVoterNode {
	return filterVoterNode(b.listNodes())
}

func filterVoterNode(members []Node) []internalVoterNode {
	voters := make([]internalVoterNode, 0, len(members))
	for _, m := range members {
		if m.IsVoter() {
			voters = append(voters, m.(internalVoterNode))
		}
	}
	return voters
}
