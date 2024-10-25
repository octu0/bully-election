package bullyelection

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
)

const (
	DefaultElectionTimeout       = 10 * time.Second
	DefaultUpdateNodeTimeout     = 5 * time.Second
	DefaultJoinNodeTimeout       = DefaultElectionTimeout + (10 * time.Second)
	DefaultLeaveNodeTimeout      = 10 * time.Second
	DefaultTransferLeaderTimeout = 10 * time.Second
	DefaultRetryNodeMsgTimeout   = 5 * time.Second
	DefaultRetryNodeEventTimeout = 5 * time.Second
)

var (
	ErrBullyInitialize           = errors.New("bully initialize")
	ErrBullyBusy                 = errors.New("bully busy")
	ErrJoinTimeout               = errors.New("join timeout")
	ErrLeaveTimeout              = errors.New("leave timeout")
	ErrTransferLeadershipTimeout = errors.New("transfer_leadership timeout")
	ErrNodeNotFound              = errors.New("node not found")
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

type (
	ObserveFunc       func(*Bully, NodeEvent, string, string)
	ULIDGeneratorFunc func() string
	OnErrorFunc       func(error)
)

// impl check
var (
	_ ObserveFunc       = DefaultObserverFunc
	_ ULIDGeneratorFunc = DefaultULIDGeneratorFunc
	_ OnErrorFunc       = DefaultOnErrorFunc
)

func DefaultObserverFunc(b *Bully, evt NodeEvent, id, addr string) {
	// nop
}

func DefaultULIDGeneratorFunc() string {
	return ulid.Make().String()
}

func DefaultOnErrorFunc(err error) {
	log.Printf("error: %+v", err)
}

type BullyOptFunc func(*bullyOpt)

type bullyOpt struct {
	observeFunc           ObserveFunc
	electionTimeout       time.Duration
	updateNodeTimeout     time.Duration
	joinNodeTimeout       time.Duration
	leaveNodeTimeout      time.Duration
	transferLeaderTimeout time.Duration
	retryNodeMsgTimeout   time.Duration
	retryNodeEventTimeout time.Duration
	ulidGeneratorFunc     ULIDGeneratorFunc
	onErrorFunc           OnErrorFunc
	logger                *log.Logger
	enableUniqNodeName    bool
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

func WithTransferLeaderTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.transferLeaderTimeout = d
	}
}

func WithRetryNodeMsgTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.retryNodeMsgTimeout = d
	}
}

func WithRetryNodeEventTimeout(d time.Duration) BullyOptFunc {
	return func(o *bullyOpt) {
		o.retryNodeEventTimeout = d
	}
}

func WithULIDGeneratorFunc(f ULIDGeneratorFunc) BullyOptFunc {
	return func(o *bullyOpt) {
		o.ulidGeneratorFunc = f
	}
}

func WithOnErrorFunc(f OnErrorFunc) BullyOptFunc {
	return func(o *bullyOpt) {
		o.onErrorFunc = f
	}
}

func WithLogger(logger *log.Logger) BullyOptFunc {
	return func(o *bullyOpt) {
		o.logger = logger
	}
}

func WithEnableUniqNodeName(enable bool) BullyOptFunc {
	return func(o *bullyOpt) {
		o.enableUniqNodeName = enable
	}
}

func newBullyOpt(opts []BullyOptFunc) *bullyOpt {
	opt := &bullyOpt{
		electionTimeout:       DefaultElectionTimeout,
		updateNodeTimeout:     DefaultUpdateNodeTimeout,
		joinNodeTimeout:       DefaultJoinNodeTimeout,
		leaveNodeTimeout:      DefaultLeaveNodeTimeout,
		transferLeaderTimeout: DefaultTransferLeaderTimeout,
		retryNodeMsgTimeout:   DefaultRetryNodeMsgTimeout,
		retryNodeEventTimeout: DefaultRetryNodeEventTimeout,
		observeFunc:           DefaultObserverFunc,
		ulidGeneratorFunc:     DefaultULIDGeneratorFunc,
		onErrorFunc:           DefaultOnErrorFunc,
		enableUniqNodeName:    true,
	}
	for _, f := range opts {
		f(opt)
	}
	return opt
}

type Bully struct {
	mu             *sync.RWMutex
	wg             *sync.WaitGroup
	electionQueue  chan *nodeEventMsg
	waitElection   chan error
	electionCancel context.CancelFunc
	stm            *stateMachine
	opt            *bullyOpt
	cancel         context.CancelFunc
	node           Node
	list           *memberlist.Memberlist
}

func (b *Bully) IsVoter() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.node.IsVoter()
}

func (b *Bully) ID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.node.ID()
}

func (b *Bully) Address() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.fulladdress()
}

func (b *Bully) fulladdress() string {
	return net.JoinHostPort(b.node.Addr(), strconv.Itoa(b.node.Port()))
}

func (b *Bully) IsLeader() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.node.IsLeader()
}

func (b *Bully) UpdateMetadata(data []byte) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.node.setUserMetadata(data)
	if err := b.list.UpdateNode(10 * time.Second); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) Members() []Node {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.listNodes()
}

func (b *Bully) listNodes() []Node {
	members := b.list.Members()
	sort.Slice(members, func(i, j int) bool {
		return members[i].Name < members[j].Name
	})

	m := make([]Node, len(members))
	for i, member := range members {
		meta, err := fromJSON(bytes.NewReader(member.Meta))
		if err != nil {
			b.opt.logger.Printf("warn: fromJSON(%s):%+v", member.Meta, errors.WithStack(err))
			continue
		}

		m[i] = nodeMetaToNode(meta)
	}
	return m
}

func (b *Bully) Join(addr string) (err error) {
	if addr == b.list.LocalNode().Address() {
		return nil // skip self join
	}
	oldLeader := b.getLeaderID()
	defer func() {
		if err != nil {
			b.setLeaderID(oldLeader)
		}
	}()
	// clear leader
	b.clearLeaderID()
	if err := b.updateNode(); err != nil {
		return errors.WithStack(err)
	}

	wait := make(chan error)
	b.mu.Lock()
	b.waitElection = wait
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.waitElection = nil
		b.mu.Unlock()
	}()

	b.opt.logger.Printf("info: join %s", addr)
	if _, err := b.list.Join([]string{addr}); err != nil {
		return errors.WithStack(err)
	}

	select {
	case err := <-wait:
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	case <-time.After(b.opt.joinNodeTimeout):
		return errors.Wrapf(ErrJoinTimeout, "timeout = %s", b.opt.joinNodeTimeout)
	}
}

func (b *Bully) Leave() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.list.Leave(b.opt.leaveNodeTimeout); err != nil {
		return errors.Wrapf(ErrLeaveTimeout, "timeout = %s: %+v", b.opt.leaveNodeTimeout, err)
	}
	return nil
}

func (b *Bully) LeadershipTransfer(ctx context.Context) error {
	isLeader := false
	b.mu.RLock()
	isLeader = b.node.IsLeader()
	b.mu.RUnlock()

	if isLeader != true {
		return nil
	}

	wait := make(chan error)
	b.mu.Lock()
	b.waitElection = wait
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.waitElection = nil
		b.mu.Unlock()
	}()

	if err := b.startLeadershipTransfer(ctx); err != nil {
		return errors.WithStack(err)
	}

	select {
	case err := <-wait:
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	case <-time.After(b.opt.transferLeaderTimeout):
		return errors.Wrapf(ErrTransferLeadershipTimeout, "timeout = %s", b.opt.transferLeaderTimeout)
	}
}

func (b *Bully) Shutdown() error {
	b.mu.Lock()
	err := b.list.Shutdown()
	b.mu.Unlock()

	if err != nil {
		return errors.WithStack(err)
	}

	b.cancel()
	b.wg.Wait()
	return nil
}

func (b *Bully) clearLeaderID() bool {
	return b.setLeaderID("")
}

func (b *Bully) setLeaderID(id string) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.setLeaderID(id)
		return true
	}
	return false
}

func (b *Bully) getLeaderID() string {
	if vn, ok := b.node.(internalVoterNode); ok {
		return vn.getLeaderID()
	}
	return ""
}

func (b *Bully) setULID(ulid string) bool {
	if vn, ok := b.node.(internalVoterNode); ok {
		vn.setULID(ulid)
		return true
	}
	return false
}

func (b *Bully) getULID() string {
	if vn, ok := b.node.(internalVoterNode); ok {
		return vn.getULID()
	}
	return ""
}

func (b *Bully) updateNode() error {
	if err := b.list.UpdateNode(b.opt.updateNodeTimeout); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) findNode(targetNodeID string) (*memberlist.Node, error) {
	for _, m := range b.list.Members() {
		if m.Name == targetNodeID {
			return m, nil
		}
	}

	return nil, errors.Wrapf(ErrNodeNotFound, "target node-id=%s", targetNodeID)
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
				if err := b.startElection(ctx); err != nil {
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

func newBully(opt *bullyOpt, node Node, list *memberlist.Memberlist, cancel context.CancelFunc) *Bully {
	return &Bully{
		mu:             new(sync.RWMutex),
		wg:             new(sync.WaitGroup),
		electionQueue:  make(chan *nodeEventMsg, 1024),
		waitElection:   nil,
		electionCancel: nopCancelFunc(),
		stm:            newStateMachine(),
		opt:            opt,
		node:           node,
		list:           list,
		cancel:         cancel,
	}
}

type createNodeFunc func(ulid string) Node

func createBully(parent context.Context, conf *memberlist.Config, funcs []BullyOptFunc, createNode createNodeFunc) (*Bully, error) {
	ctx, cancel := context.WithCancel(parent)
	opt := newBullyOpt(funcs)
	if opt.logger == nil {
		opt.logger = log.New(os.Stderr, conf.Name+" ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	}

	ulid := opt.ulidGeneratorFunc()
	if opt.enableUniqNodeName {
		conf.Name = fmt.Sprintf("%s-%s", conf.Name, ulid)
	}

	ready := newAtomicReadyStatus()
	msgCh := make(chan []byte)
	evtCh := make(chan *nodeEventMsg)

	resolvLater := false
	if conf.BindPort == 0 && conf.AdvertisePort == 0 {
		resolvLater = true
	}

	node := createNode(ulid)
	conf.Delegate = newObserveNodeMessage(ctx, opt, ready, msgCh, node)
	conf.Events = newObserveNodeEvent(ctx, opt, ready, evtCh)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}
	if resolvLater {
		node.setPort(int(list.LocalNode().Port))
		opt.logger.SetPrefix(fmt.Sprintf("%s(%s:%d) ", node.ID(), node.Addr(), node.Port()))
	}

	b := newBully(opt, node, list, cancel)
	b.wg.Add(3)
	go b.readNodeMessageLoop(ctx, msgCh, evtCh)
	go b.readNodeEventLoop(ctx, evtCh)
	go b.electionRunLoop(ctx)

	if ready.setStatusOk() != true {
		cancel()
		return nil, errors.Wrapf(ErrBullyInitialize, "already initialized")
	}

	return b, nil
}

func CreateVoter(parent context.Context, conf *memberlist.Config, funcs ...BullyOptFunc) (*Bully, error) {
	return createBully(parent, conf, funcs, func(ulid string) Node {
		return newVoterNode(conf.Name, ulid, conf.AdvertiseAddr, conf.AdvertisePort, stateInitial.String())
	})
}

func CreateNonVoter(parent context.Context, conf *memberlist.Config, funcs ...BullyOptFunc) (*Bully, error) {
	return createBully(parent, conf, funcs, func(ulid string) Node {
		return newNonvoterNode(conf.Name, ulid, conf.AdvertiseAddr, conf.AdvertisePort)
	})
}

type readyStatus int32

const (
	readyInit readyStatus = iota
	readyOk
)

type atomicReadyStatus struct {
	v int32
}

func (a *atomicReadyStatus) setStatusOk() bool {
	return atomic.CompareAndSwapInt32(&a.v, int32(readyInit), int32(readyOk))
}

func (a *atomicReadyStatus) IsOk() bool {
	return atomic.LoadInt32(&a.v) == int32(readyOk)
}

func newAtomicReadyStatus() *atomicReadyStatus {
	return &atomicReadyStatus{int32(readyInit)}
}

func nopCancelFunc() context.CancelFunc {
	return context.CancelFunc(func() {
		// nop
	})
}
