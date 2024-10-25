package bullyelection

import (
	"context"

	"github.com/pkg/errors"
)

type NodeEvent uint8

const (
	JoinEvent NodeEvent = iota + 1
	LeaveEvent
	TransferLeadershipEvent
	ElectionEvent
)

func (evt NodeEvent) String() string {
	switch evt {
	case JoinEvent:
		return "join"
	case LeaveEvent:
		return "leave"
	case TransferLeadershipEvent:
		return "transfer_leadership"
	case ElectionEvent:
		return "election"
	}
	return "unknown event"
}

func (b *Bully) readNodeEventLoop(ctx context.Context, ch chan *nodeEventMsg) {
	defer b.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-ch:
			select {
			case b.electionQueue <- msg:
				// ok
			default:
				b.opt.onErrorFunc(errors.Wrapf(ErrBullyBusy, "maybe hangup election, drop: %s", msg))
			}
		}
	}
}

func (b *Bully) electionRunLoop(ctx context.Context) {
	defer b.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-b.electionQueue:
			switch msg.evt {
			case JoinEvent, LeaveEvent, TransferLeadershipEvent:
				if err := b.startElection(ctx, msg.evt, msg.id); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "election failure"))
					continue
				}
				b.opt.observeFunc(b, msg.evt, msg.id, msg.addr)
			case ElectionEvent:
				// emit only
				b.opt.observeFunc(b, msg.evt, msg.id, msg.addr)
			}
		}
	}
}
