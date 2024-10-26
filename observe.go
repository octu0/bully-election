package bullyelection

import (
	"bytes"
	"context"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
)

var (
	_ memberlist.Delegate = (*observeNodeMessage)(nil)
)

type observeNodeMessage struct {
	ctx   context.Context
	opt   *bullyOpt
	ready *atomicReadyStatus
	msgCh chan []byte
	node  Node
}

func (d *observeNodeMessage) GetBroadcasts(overhead int, limit int) [][]byte {
	// nop
	return nil
}

func (d *observeNodeMessage) LocalState(join bool) []byte {
	// nop
	return nil
}

func (d *observeNodeMessage) MergeRemoteState(buf []byte, join bool) {
	// nop
}

func (d *observeNodeMessage) NodeMeta(limit int) []byte {
	buf := bytes.NewBuffer(nil)
	if err := d.node.toJSON(buf); err != nil {
		d.opt.logger.Printf("warn: node.ToJSON %+v", errors.WithStack(err))
		return nil
	}
	return buf.Bytes()
}

func (d *observeNodeMessage) NotifyMsg(msg []byte) {
	if d.ready.IsOk() != true {
		return // drop
	}

	select {
	case <-d.ctx.Done():
		return

	case d.msgCh <- msg:
		// ok
	case <-time.After(d.opt.retryNodeMsgTimeout):
		d.opt.logger.Printf("warn: msgCh maybe hangup, drop msg: %s", msg)
	}
}

func newObserveNodeMessage(ctx context.Context, opt *bullyOpt, ready *atomicReadyStatus, ch chan []byte, node Node) *observeNodeMessage {
	return &observeNodeMessage{ctx, opt, ready, ch, node}
}

var (
	_ memberlist.EventDelegate = (*observeNodeEvent)(nil)
)

type nodeEventMsg struct {
	evt     NodeEvent
	id      string
	addr    string
	isVoter bool
}

type observeNodeEvent struct {
	ctx   context.Context
	opt   *bullyOpt
	ready *atomicReadyStatus
	evtCh chan *nodeEventMsg
}

func (e *observeNodeEvent) NotifyJoin(node *memberlist.Node) {
	if e.ready.IsOk() != true {
		return // drop
	}

	e.opt.logger.Printf("info: join event: name=%s addr=%s meta=%s", node.Name, node.Address(), node.Meta)
	meta, err := fromJSON(bytes.NewReader(node.Meta))
	if err != nil {
		e.opt.logger.Printf("warn: fromJSON(%s):%+v", node.Meta, errors.WithStack(err))
		return
	}

	msg := &nodeEventMsg{JoinEvent, node.Name, node.Address(), meta.IsVoter}
	select {
	case <-e.ctx.Done():
		return

	case e.evtCh <- msg:
		// ok
	case <-time.After(e.opt.retryNodeEventTimeout):
		e.opt.logger.Printf("warn: evtCh maybe hangup(join), drop msg: %+v", msg)
	}
}

func (e *observeNodeEvent) NotifyLeave(node *memberlist.Node) {
	if e.ready.IsOk() != true {
		return // drop
	}

	e.opt.logger.Printf("info: leave event: name=%s addr=%s", node.Name, node.Address())
	meta, err := fromJSON(bytes.NewReader(node.Meta))
	if err != nil {
		e.opt.logger.Printf("warn: fromJSON(%s):%+v", node.Meta, errors.WithStack(err))
		return
	}

	msg := &nodeEventMsg{LeaveEvent, node.Name, node.Address(), meta.IsVoter}
	select {
	case <-e.ctx.Done():
		return

	case e.evtCh <- msg:
		// ok
	case <-time.After(e.opt.retryNodeEventTimeout):
		e.opt.logger.Printf("warn: evtCh maybe hangup(leave), drop msg: %+v", msg)
	}
}

func (e *observeNodeEvent) NotifyUpdate(node *memberlist.Node) {
	// nop
}

func newObserveNodeEvent(ctx context.Context, opt *bullyOpt, ready *atomicReadyStatus, ch chan *nodeEventMsg) *observeNodeEvent {
	return &observeNodeEvent{ctx, opt, ready, ch}
}
