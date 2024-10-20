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

const (
	DefaultElectionTimeout   = 30 * time.Second
	DefaultElectionInterval  = 100 * time.Millisecond
	DefaultUpdateNodeTimeout = 3 * time.Second
)

var (
	errNodeNotFound = errors.New("node not found")
)

var (
	bufferPool = &sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
)

type NodeEvent uint8

const (
	JoinEvent NodeEvent = iota + 1
	LeaveEvent
	UpdateEvent
)

type ObserveFunc func(*Bully, NodeEvent)

type NodeNumberGeneratorFunc func(string) int64

func DefaultNodeNumberGenerator(string) int64 {
	return time.Now().UnixNano()
}

type BullyOptFunc func(*bullyOpt)

type bullyOpt struct {
	observeFunc       ObserveFunc
	electionTimeout   time.Duration
	electionInterval  time.Duration
	updateNodeTimeout time.Duration
	nodeNumberGenFunc NodeNumberGeneratorFunc
}

func WithObserveFunc(f ObserveFunc) BullyOptFunc {
	return func(o *bullyOpt) {
		o.observeFunc = f
	}
}

func WithElectionTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.electionTimeout = d
	}
}

func WithElectionInterval(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.electionInterval = d
	}
}

func WithUpdateNodeTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.updateNodeTimeout = d
	}
}

func newBullyOpt(opts []BullyOptFunc) *bullyOpt {
	opt := &bullyOpt{
		electionTimeout:   DefaultElectionTimeout,
		electionInterval:  DefaultElectionInterval,
		updateNodeTimeout: DefaultUpdateNodeTimeout,
	}
	for _, f := range opts {
		f(opt)
	}
	if opt.observeFunc == nil {
		opt.observeFunc = func(b *Bully, evt NodeEvent) {
			// nop
		}
	}
	if opt.nodeNumberGenFunc == nil {
		opt.nodeNumberGenFunc = DefaultNodeNumberGenerator
	}
	return opt
}

type Bully struct {
	opt    *bullyOpt
	wg     *sync.WaitGroup
	cancel context.CancelFunc
	node   Node
	list   *memberlist.Memberlist
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

func (b *Bully) UpdateMetadata(data []byte) error {
	b.node.setUserMetadata(data)
	if err := b.list.UpdateNode(10 * time.Second); err != nil {
		return errors.WithStack(err)
	}
	return nil
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

func (b *Bully) TransferLeader(ctx context.Context) error {
	if b.node.IsLeader() != true {
		return nil
	}

	if err := startTransferLeader(ctx, b); err != nil {
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

func (b *Bully) setState(newState State) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.setState(newState.String())
		return true
	}
	return false
}

func (b *Bully) setLeaderID(id string) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.setLeaderID(id)
		return true
	}
	return false
}

func (b *Bully) resetVote() bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.resetVote()
		return true
	}
	return false
}

func (b *Bully) incrementElection(n uint32) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.incrementElection(n)
		return true
	}
	return false
}

func (b *Bully) incrementAnswer(n uint32) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.incrementAnswer(n)
		return true
	}
	return false
}

func (b *Bully) updateNode() error {
	if err := b.list.UpdateNode(b.opt.updateNodeTimeout); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) setNodeNumber(newNumber int64) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.setNodeNumber(newNumber)
		return true
	}
	return false
}

func (b *Bully) getNodeNumber() int64 {
	if vn, ok := b.node.(internalVoterNode); ok {
		return vn.getNodeNumber()
	}
	return -1
}

func (b *Bully) findNode(targetNodeID string) (*memberlist.Node, error) {
	for _, m := range b.list.Members() {
		if m.Name == targetNodeID {
			return m, nil
		}
	}

	return nil, errors.Wrapf(errNodeNotFound, "target node-id=%s", targetNodeID)
}

func (b *Bully) send(targetNodeID string, data []byte) error {
	targetNode, err := b.findNode(targetNodeID)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := b.list.SendReliable(targetNode, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) sendElection(targetNodeID string) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	if err := marshalMessage(buf, ElectionMessage, b.ID()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) sendAnswer(targetNodeID string) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	if err := marshalMessage(buf, AnswerMessage, b.ID()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) sendCoordinator(targetNodeID string) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	if err := marshalMessage(buf, CoordinatorMessage, b.ID()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) sendTransferLeader(targetNodeID string) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	if err := marshalMessage(buf, TransferLeaderMessage, b.ID()); err != nil {
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
			if err := handleMessage(ctx, b, msg); err != nil {
				log.Printf("error: handle message: %+v", err)
				continue
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
				b.opt.observeFunc(b, msg.evt)
			case UpdateEvent:
				// nop
			}
		}
	}
}

func CreateVoter(parent context.Context, conf *memberlist.Config, funcs ...BullyOptFunc) (*Bully, error) {
	opt := newBullyOpt(funcs)

	ctx, cancel := context.WithCancel(parent)
	msgCh := make(chan []byte, 0)
	evtCh := make(chan *hookNodeEventMsg, 0)

	nodeNum := opt.nodeNumberGenFunc(conf.Name)
	node := NewVoterNode(conf.Name, conf.AdvertiseAddr, conf.AdvertisePort, StateInitial.String(), nodeNum)
	conf.Delegate = newHookMessage(msgCh, node)
	conf.Events = newHooNodeEvent(evtCh)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}

	b := &Bully{
		opt:    opt,
		wg:     new(sync.WaitGroup),
		cancel: cancel,
		node:   node,
		list:   list,
	}
	b.wg.Add(2)
	go b.readMessageLoop(ctx, msgCh)
	go b.readNodeEventLoop(ctx, evtCh)

	return b, nil
}

func CreateNonVoter(parent context.Context, conf *memberlist.Config, funcs ...BullyOptFunc) (*Bully, error) {
	opt := newBullyOpt(funcs)

	ctx, cancel := context.WithCancel(parent)
	msgCh := make(chan []byte, 0)

	node := NewNonvoterNode(conf.Name, conf.AdvertiseAddr, conf.AdvertisePort)
	conf.Delegate = newHookMessage(msgCh, node)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}

	b := &Bully{
		opt:    opt,
		wg:     new(sync.WaitGroup),
		cancel: cancel,
		node:   node,
		list:   list,
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
}

func (d *hookMessage) GetBroadcasts(overhead int, limit int) [][]byte {
	// nop
	return nil
}

func (d *hookMessage) LocalState(join bool) []byte {
	// nop
	return nil
}

func (d *hookMessage) MergeRemoteState(buf []byte, join bool) {
	// nop
}

func (d *hookMessage) NodeMeta(limit int) []byte {
	d.Lock()
	defer d.Unlock()

	buf := bytes.NewBuffer(nil)
	if err := d.node.toJSON(buf); err != nil {
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

func newHookMessage(ch chan []byte, node Node) *hookMessage {
	return &hookMessage{
		msgCh: ch,
		node:  node,
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
}

func newHooNodeEvent(ch chan *hookNodeEventMsg) *hookNodeEvent {
	return &hookNodeEvent{evtCh: ch}
}
