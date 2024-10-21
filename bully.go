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
	DefaultJoinNodeTimeout   = 3 * time.Second
	DefaultLeaveNodeTimeout  = 3 * time.Second
)

var (
	ErrBullyInitialize   = errors.New("bully initialize")
	ErrBullyAliveTimeout = errors.New("bully alive timeout")
	ErrJoinTimeout       = errors.New("join timeout")
	ErrLeaveTimeout      = errors.New("leave timeout")
	errNodeNotFound      = errors.New("node not found")
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

func (evt NodeEvent) String() string {
	switch evt {
	case JoinEvent:
		return "join"
	case LeaveEvent:
		return "leave"
	case UpdateEvent:
		return "update"
	}
	return "unknown event"
}

type (
	ObserveFunc             func(*Bully, NodeEvent)
	NodeNumberGeneratorFunc func(string) int64
	OnErrorFunc             func(error)
)

func DefaultObserverFunc(b *Bully, evt NodeEvent) {
	// nop
}

func DefaultNodeNumberGeneratorFunc(string) int64 {
	return time.Now().UnixNano()
}

func DefaultOnErrorFunc(err error) {
	log.Printf("error: %+v", err)
}

type BullyOptFunc func(*bullyOpt)

type bullyOpt struct {
	observeFunc       ObserveFunc
	electionTimeout   time.Duration
	electionInterval  time.Duration
	updateNodeTimeout time.Duration
	joinNodeTimeout   time.Duration
	leaveNodeTimeout  time.Duration
	nodeNumberGenFunc NodeNumberGeneratorFunc
	onErrorFunc       OnErrorFunc
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

func WithJoinNodeTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.joinNodeTimeout = d
	}
}

func WithLeaveNodeTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.leaveNodeTimeout = d
	}
}

func WithNodeNumberGeneratorFunc(f NodeNumberGeneratorFunc) BullyOptFunc {
	return func(o *bullyOpt) {
		o.nodeNumberGenFunc = f
	}
}

func WithOnErrorFunc(f OnErrorFunc) BullyOptFunc {
	return func(o *bullyOpt) {
		o.onErrorFunc = f
	}
}

func newBullyOpt(opts []BullyOptFunc) *bullyOpt {
	opt := &bullyOpt{
		electionTimeout:   DefaultElectionTimeout,
		electionInterval:  DefaultElectionInterval,
		updateNodeTimeout: DefaultUpdateNodeTimeout,
		joinNodeTimeout:   DefaultJoinNodeTimeout,
		leaveNodeTimeout:  DefaultLeaveNodeTimeout,
		observeFunc:       DefaultObserverFunc,
		nodeNumberGenFunc: DefaultNodeNumberGeneratorFunc,
		onErrorFunc:       DefaultOnErrorFunc,
	}
	for _, f := range opts {
		f(opt)
	}
	return opt
}

type Bully struct {
	opt         *bullyOpt
	mu          *sync.RWMutex
	wg          *sync.WaitGroup
	onceStartup *sync.Once
	waitStartup chan struct{}
	joinWait    map[string]chan struct{}
	leaveWait   map[string]chan struct{}
	cancel      context.CancelFunc
	node        Node
	list        *memberlist.Memberlist
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
	if addr == b.list.LocalNode().Address() {
		return nil // skip self join (memberlist no event)
	}

	wait := make(chan struct{})
	b.mu.Lock()
	b.joinWait[addr] = wait
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.joinWait, addr)
		b.mu.Unlock()
	}()

	ret := make(chan error)
	go func() {
		select {
		case <-time.After(b.opt.joinNodeTimeout * 2):
			ret <- errors.Wrapf(ErrJoinTimeout, "memberlist joined but not call callback")
		case <-wait:
			ret <- nil // ok
		}
	}()

	if _, err := b.list.Join([]string{addr}); err != nil {
		return errors.WithStack(err)
	}

	return <-ret
}

func (b *Bully) Leave() error {
	addr := b.list.LocalNode().Address()
	wait := make(chan struct{})
	b.mu.Lock()
	b.leaveWait[addr] = wait
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.leaveWait, addr)
		b.mu.Unlock()
	}()

	ret := make(chan error)
	go func() {
		select {
		case <-time.After(b.opt.leaveNodeTimeout * 2):
			ret <- errors.Wrapf(ErrLeaveTimeout, "memberlist leaved but not call callback")
		case <-wait:
			ret <- nil // ok
		}
	}()

	if err := b.list.Leave(b.opt.leaveNodeTimeout); err != nil {
		return errors.WithStack(err)
	}
	return <-ret
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
				b.opt.onErrorFunc(errors.Wrapf(err, "recv data: %s", data))
				continue
			}
			if err := handleMessage(ctx, b, msg); err != nil {
				b.opt.onErrorFunc(errors.Wrapf(err, "handle message: %+v", msg))
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
			case JoinEvent:
				if err := startElection(ctx, b); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "election failure"))
					continue
				}
				if msg.node.Name == b.node.ID() {
					b.onceStartup.Do(func() {
						select {
						case b.waitStartup <- struct{}{}:
							// ok
						default:
							// pass
						}
					})
				}

				b.mu.RLock()
				if ch, ok := b.joinWait[msg.node.Address()]; ok {
					select {
					case ch <- struct{}{}:
						// ok
					default:
						log.Printf("warn: join wait exits but has no reader")
					}
				}
				b.mu.RUnlock()

				b.opt.observeFunc(b, msg.evt)

			case LeaveEvent:
				if msg.node.Name != b.node.ID() { // leave other node = run election
					if err := startElection(ctx, b); err != nil {
						b.opt.onErrorFunc(errors.Wrapf(err, "election failure"))
						continue
					}
				}

				b.mu.RLock()
				if ch, ok := b.leaveWait[msg.node.Address()]; ok {
					select {
					case ch <- struct{}{}:
						// ok
					default:
						log.Printf("warn: leave wait exists but has no reader")
					}
				}
				b.mu.RUnlock()

				b.opt.observeFunc(b, msg.evt)
			}
		}
	}
}

func (b *Bully) waitSelfJoin(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ErrBullyInitialize)
	case <-time.After(b.opt.electionTimeout):
		return errors.WithStack(ErrBullyAliveTimeout)
	case <-b.waitStartup:
		return nil // ok
	}
}

func newBully(cancel context.CancelFunc, node Node, list *memberlist.Memberlist, opt *bullyOpt) *Bully {
	return &Bully{
		opt:         opt,
		mu:          new(sync.RWMutex),
		wg:          new(sync.WaitGroup),
		onceStartup: new(sync.Once),
		waitStartup: make(chan struct{}),
		joinWait:    make(map[string]chan struct{}),
		leaveWait:   make(map[string]chan struct{}),
		cancel:      cancel,
		node:        node,
		list:        list,
	}
}

func CreateVoter(parent context.Context, conf *memberlist.Config, funcs ...BullyOptFunc) (*Bully, error) {
	opt := newBullyOpt(funcs)

	ctx, cancel := context.WithCancel(parent)
	msgCh := make(chan []byte, 1)            // 1 = startup election
	evtCh := make(chan *hookNodeEventMsg, 1) // 1 = on creation join msg

	resolvLater := false
	if conf.BindPort == 0 && conf.AdvertisePort == 0 {
		resolvLater = true
	}

	nodeNum := opt.nodeNumberGenFunc(conf.Name)
	node := newVoterNode(conf.Name, conf.AdvertiseAddr, conf.AdvertisePort, StateInitial.String(), nodeNum)
	conf.Delegate = newHookMessage(msgCh, node)
	conf.Events = newHooNodeEvent(evtCh)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}
	if resolvLater {
		node.port = int(list.LocalNode().Port)
	}

	b := newBully(cancel, node, list, opt)
	b.wg.Add(2)
	go b.readMessageLoop(ctx, msgCh)
	go b.readNodeEventLoop(ctx, evtCh)

	if err := b.waitSelfJoin(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return b, nil
}

func CreateNonVoter(parent context.Context, conf *memberlist.Config, funcs ...BullyOptFunc) (*Bully, error) {
	opt := newBullyOpt(funcs)

	ctx, cancel := context.WithCancel(parent)
	msgCh := make(chan []byte, 1)

	resolvLater := false
	if conf.BindPort == 0 || conf.AdvertisePort == 0 {
		resolvLater = true
	}

	node := newNonvoterNode(conf.Name, conf.AdvertiseAddr, conf.AdvertisePort)
	conf.Delegate = newHookMessage(msgCh, node)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}
	if resolvLater {
		node.port = int(list.LocalNode().Port)
	}

	b := newBully(cancel, node, list, opt)
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
	return &hookMessage{msgCh: ch, node: node}
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

	log.Printf("info: join event: name=%s addr=%s", node.Name, node.Address())
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

	log.Printf("info: leave event: name=%s addr=%s", node.Name, node.Address())
	msg := &hookNodeEventMsg{LeaveEvent, node}
	select {
	case e.evtCh <- msg:
		// ok
	default:
		log.Printf("warn: evtCh maybe hangup(leave), drop msg: %+v", msg)
	}
}

func (e *hookNodeEvent) NotifyUpdate(node *memberlist.Node) {
	// nop
}

func newHooNodeEvent(ch chan *hookNodeEventMsg) *hookNodeEvent {
	return &hookNodeEvent{evtCh: ch}
}
