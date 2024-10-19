package bullyelection

import (
	"bytes"
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
)

var (
	errNodeNotFound = errors.New("node not found")
)

type State string

const (
	StateInitial  State = "init"
	StateElecting State = "electing"
	StateAnswered State = "answered"
	StateElected  State = "elected"
)

func (s State) String() string {
	return string(s)
}

type NodeEvent uint8

const (
	JoinEvent NodeEvent = iota + 1
	LeaveEvent
	UpdateEvent
)

type ObserveFunc func(*Bully, NodeEvent)

type Bully struct {
	wg          *sync.WaitGroup
	cancel      context.CancelFunc
	node        Node
	list        *memberlist.Memberlist
	observeFunc ObserveFunc
}

func (b *Bully) IsVoter() bool {
	return b.node.IsVoterNode()
}

func (b *Bully) ID() string {
	return b.node.ID()
}

func (b *Bully) Address() string {
	return net.JoinHostPort(b.node.Addr(), strconv.Itoa(b.node.Port()))
}

func (b *Bully) IsLeader() bool {
	return b.node.IsLeader()
}

func (b *Bully) Members() []Node {
	members := b.list.Members()
	m := make([]Node, len(members))
	for i, member := range members {
		meta, err := fromJSON(bytes.NewReader(member.Meta))
		if err != nil {
			log.Printf("warn: fromJSON(%s):%+v", member.Meta, errors.WithStack(err))
			continue
		}

		m[i] = nodeMetaToNode(meta)
	}
	return m
}

func (b *Bully) Join(addr string) error {
	if _, err := b.list.Join([]string{addr}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) Leave(timeout time.Duration) error {
	if err := b.list.Leave(timeout); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) Shutdown() error {
	if err := b.list.Shutdown(); err != nil {
		return errors.WithStack(err)
	}
	b.cancel()
	b.wg.Wait()
	return nil
}

func (b *Bully) setState(newState State) {
	b.node.setState(newState.String())
}

func (b *Bully) resetVote() {
	b.node.resetVote()
}

func (b *Bully) updateNode(timeout time.Duration) error {
	if err := b.list.UpdateNode(timeout); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) send(targetNodeID string, data []byte) error {
	targetNode, err := func() (*memberlist.Node, error) {
		for _, m := range b.list.Members() {
			if m.Name == targetNodeID {
				return m, nil
			}
		}

		return nil, errors.Wrapf(errNodeNotFound, "target node-id=%s", targetNodeID)
	}()
	if err != nil {
		return errors.WithStack(err)
	}

	if err := b.list.SendReliable(targetNode, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) sendElection(targetNodeID string) error {
	buf := bytes.NewBuffer(nil)
	if err := marshalMessage(buf, ElectionMessage, b.ID()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) sendAnswer(targetNodeID string) error {
	buf := bytes.NewBuffer(nil)
	if err := marshalMessage(buf, AnswerMessage, b.ID()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) sendCoordinator(targetNodeID string) error {
	buf := bytes.NewBuffer(nil)
	if err := marshalMessage(buf, CoordinatorMessage, b.ID()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) readMessageLoop(ctx context.Context, ch chan []byte) {
	defer b.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-ch:
			msg, err := unmarshalMessage(bytes.NewReader(data))
			if err != nil {
				log.Printf("error: recv data(%s): %+v", data, err)
				continue
			}

			switch msg.Type {
			case ElectionMessage:
				if err := b.sendAnswer(msg.NodeID); err != nil {
					log.Printf("error: send answer(%s): %+v", msg.NodeID, err)
					continue
				}
				b.node.incrementElection(1)
				b.setState(StateAnswered)
				if err := b.updateNode(10 * time.Second); err != nil {
					log.Printf("error: updateNode: %+v", err)
					continue
				}

			case AnswerMessage:
				b.node.incrementAnswer(1)
				if err := b.updateNode(10 * time.Second); err != nil {
					log.Printf("error: updateNode: %+v", err)
					continue
				}

			case CoordinatorMessage:
				b.node.setLeaderID(msg.NodeID)
				b.setState(StateElected)
				if err := b.updateNode(10 * time.Second); err != nil {
					log.Printf("error: updateNode: %+v", err)
					continue
				}
			}
		}
	}
}

func (b *Bully) readNodeEventLoop(ctx context.Context, ch chan *hookNodeEventMsg) {
	defer b.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			switch msg.evt {
			case JoinEvent, LeaveEvent:
				if err := startElection(ctx, b); err != nil {
					log.Printf("error: election failure: %+v", err)
				}
			}
			b.observeFunc(b, msg.evt)
		}
	}
}

func CreateVoter(parent context.Context, conf *memberlist.Config, observeFunc ObserveFunc) (*Bully, error) {
	ctx, cancel := context.WithCancel(parent)
	msgCh := make(chan []byte, 0)
	evtCh := make(chan *hookNodeEventMsg, 0)

	node := NewVoterNode(conf.Name, conf.AdvertiseAddr, conf.AdvertisePort, StateInitial.String())
	orgDelegate := conf.Delegate
	orgEvents := conf.Events
	conf.Delegate = newHookMessage(msgCh, node, orgDelegate)
	conf.Events = newHooNodeEvent(evtCh, orgEvents)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}

	b := &Bully{
		wg:          new(sync.WaitGroup),
		cancel:      cancel,
		node:        node,
		list:        list,
		observeFunc: observeFunc,
	}
	b.wg.Add(2)
	go b.readMessageLoop(ctx, msgCh)
	go b.readNodeEventLoop(ctx, evtCh)

	return b, nil
}

func CreateNonVoter(parent context.Context, conf *memberlist.Config, observeFunc ObserveFunc) (*Bully, error) {
	ctx, cancel := context.WithCancel(parent)
	msgCh := make(chan []byte, 0)

	node := NewNonvoterNode(conf.Name, conf.AdvertiseAddr, conf.AdvertisePort)
	orgDelegate := conf.Delegate
	conf.Delegate = newHookMessage(msgCh, node, orgDelegate)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}

	b := &Bully{
		wg:          new(sync.WaitGroup),
		cancel:      cancel,
		node:        node,
		list:        list,
		observeFunc: observeFunc,
	}
	b.wg.Add(1)
	go b.readMessageLoop(ctx, msgCh)

	return b, nil
}

var (
	_ memberlist.Delegate = (*hookMessage)(nil)
)

type hookMessage struct {
	sync.Mutex

	msgCh chan []byte
	node  Node
	orig  memberlist.Delegate
}

func (d *hookMessage) GetBroadcasts(overhead int, limit int) [][]byte {
	d.Lock()
	defer d.Unlock()

	if d.orig != nil {
		return d.orig.GetBroadcasts(overhead, limit)
	}
	return nil
}

func (d *hookMessage) LocalState(join bool) []byte {
	d.Lock()
	defer d.Unlock()

	buf := bytes.NewBuffer(nil)
	buf.WriteString(d.node.State())
	if join {
		buf.WriteString("-joined")
	}
	if d.orig != nil {
		st := d.orig.LocalState(join)
		buf.WriteString("-")
		buf.Write(st)
	}
	return buf.Bytes()
}

func (d *hookMessage) MergeRemoteState(buf []byte, join bool) {
	d.Lock()
	defer d.Unlock()

	if d.orig != nil {
		d.orig.MergeRemoteState(buf, join)
	}
}

func (d *hookMessage) NodeMeta(limit int) []byte {
	d.Lock()
	defer d.Unlock()

	buf := bytes.NewBuffer(nil)
	arib := func() []byte {
		if d.orig != nil {
			return d.orig.NodeMeta(limit)
		}
		return nil
	}()

	if err := d.node.ToJSON(buf, arib); err != nil {
		log.Printf("warn: node.ToJSON %+v", errors.WithStack(err))
		return nil
	}
	return buf.Bytes()
}

func (d *hookMessage) NotifyMsg(msg []byte) {
	d.Lock()
	defer d.Unlock()

	select {
	case d.msgCh <- msg:
		// ok
	default:
		log.Printf("warn: msgCh maybe hangup, drop msg: %s", msg)
	}
}

func newHookMessage(ch chan []byte, node Node, orgDelegate memberlist.Delegate) *hookMessage {
	return &hookMessage{
		msgCh: ch,
		node:  node,
		orig:  orgDelegate,
	}
}

var (
	_ memberlist.EventDelegate = (*hookNodeEvent)(nil)
)

type hookNodeEventMsg struct {
	evt  NodeEvent
	node *memberlist.Node
}

type hookNodeEvent struct {
	sync.Mutex

	evtCh chan *hookNodeEventMsg
	orig  memberlist.EventDelegate
}

func (e *hookNodeEvent) NotifyJoin(node *memberlist.Node) {
	e.Lock()
	defer e.Unlock()

	msg := &hookNodeEventMsg{JoinEvent, node}
	select {
	case e.evtCh <- msg:
		// ok
	default:
		log.Printf("warn: evtCh maybe hangup(join), drop msg: %+v", msg)
	}

	if e.orig != nil {
		e.orig.NotifyJoin(node)
	}
}

func (e *hookNodeEvent) NotifyLeave(node *memberlist.Node) {
	e.Lock()
	defer e.Unlock()

	msg := &hookNodeEventMsg{LeaveEvent, node}
	select {
	case e.evtCh <- msg:
		// ok
	default:
		log.Printf("warn: evtCh maybe hangup(leave), drop msg: %+v", msg)
	}

	if e.orig != nil {
		e.orig.NotifyLeave(node)
	}
}

func (e *hookNodeEvent) NotifyUpdate(node *memberlist.Node) {
	e.Lock()
	defer e.Unlock()

	msg := &hookNodeEventMsg{UpdateEvent, node}
	select {
	case e.evtCh <- msg:
		// ok
	default:
		log.Printf("warn: evtCh maybe hangup(update), drop msg: %+v", msg)
	}

	if e.orig != nil {
		e.orig.NotifyUpdate(node)
	}
}

func newHooNodeEvent(ch chan *hookNodeEventMsg, orig memberlist.EventDelegate) *hookNodeEvent {
	return &hookNodeEvent{
		evtCh: ch,
		orig:  orig,
	}
}
