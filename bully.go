package bullyelection

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
)

const (
	DefaultElectionTimeout         = 10 * time.Second
	DefaultElectionInterval        = 1 * time.Millisecond
	DefaultUpdateNodeTimeout       = 5 * time.Second
	DefaultJoinNodeTimeout         = 10 * time.Second
	DefaultLeaveNodeTimeout        = 10 * time.Second
	DefaultTransferLeaderTimeout   = 10 * time.Second
	DefaultRetryNodeMsgTimeout     = 15 * time.Second
	DefaultRetryNodeEventTimeout   = 30 * time.Second
	DefaultRetryNodeEventsInterval = 1 * time.Millisecond
	DefaultStepInterval            = 1 * time.Second // workaround
)

var (
	ErrBullyInitialize           = errors.New("bully initialize")
	ErrBullyAliveTimeout         = errors.New("bully alive timeout")
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
)

func (evt NodeEvent) String() string {
	switch evt {
	case JoinEvent:
		return "join"
	case LeaveEvent:
		return "leave"
	case TransferLeadershipEvent:
		return "transfer_leadership"
	}
	return "unknown event"
}

type electionState string

const (
	stateInitial            electionState = "init"
	stateRunning            electionState = "running"
	stateWaitElection       electionState = "wait_election"
	stateElecting           electionState = "electing"
	stateTransferLeadership electionState = "transfer_leader"
)

func (s electionState) String() string {
	return string(s)
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
	electionInterval      time.Duration
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
		electionInterval:      DefaultElectionInterval,
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
	opt            *bullyOpt
	mu             *sync.RWMutex
	wg             *sync.WaitGroup
	ready          *atomicReadyStatus
	waitElection   chan error
	recvReadyCount chan *recvReadyElectionCount
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

func (b *Bully) State() electionState {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if vn, ok := b.node.(internalVoterNode); ok {
		return electionState(vn.getState())
	}
	return ""
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
	b.setLeaderID("")

	wait := make(chan error)
	b.mu.Lock()
	b.waitElection = wait
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.waitElection = nil
		b.mu.Unlock()
	}()

	// default state = electing
	if err := func() error {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.setState(stateElecting)
		if err := b.updateNode(); err != nil {
			return errors.Wrapf(ErrBullyInitialize, "state change : %s", stateElecting)
		}
		return nil
	}(); err != nil {
		return errors.WithStack(err)
	}

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

func (b *Bully) setState(newState electionState) bool {
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

func (b *Bully) updateNode() error {
	if err := b.list.UpdateNode(b.opt.updateNodeTimeout); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bully) syncState(newState electionState) error {
	if b.setState(newState) {
		if err := b.updateNode(); err != nil {
			return errors.WithStack(err)
		}
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
			switch msg.evt {
			case JoinEvent, LeaveEvent, TransferLeadershipEvent:
				if err := b.startElection(ctx); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "election failure"))
					continue
				}
				b.opt.observeFunc(b, msg.evt, msg.id, msg.addr)
			}
		}
	}
}

func (b *Bully) readyChannel(_ context.Context) error {
	if b.ready.setStatusOk() != true {
		return errors.Wrapf(ErrBullyInitialize, "already initialized")
	}
	return nil
}

func newBully(readyStatus *atomicReadyStatus, cancel context.CancelFunc, node Node, list *memberlist.Memberlist, opt *bullyOpt) *Bully {
	return &Bully{
		opt:            opt,
		mu:             new(sync.RWMutex),
		wg:             new(sync.WaitGroup),
		ready:          readyStatus,
		cancel:         cancel,
		node:           node,
		list:           list,
		recvReadyCount: make(chan *recvReadyElectionCount),
	}
}

type createNodeFunc func(ulid string) Node

func createBully(parent context.Context, conf *memberlist.Config, funcs []BullyOptFunc, createNode createNodeFunc) (*Bully, error) {
	opt := newBullyOpt(funcs)
	if opt.logger == nil {
		opt.logger = log.New(os.Stderr, conf.Name+" ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	}

	ulid := opt.ulidGeneratorFunc()
	if opt.enableUniqNodeName {
		conf.Name = fmt.Sprintf("%s-%s", conf.Name, ulid)
	}

	ctx, cancel := context.WithCancel(parent)
	ready := newAtomicReadyStatus()
	msgCh := make(chan []byte)
	evtCh := make(chan *nodeEventMsg)

	resolvLater := false
	if conf.BindPort == 0 && conf.AdvertisePort == 0 {
		resolvLater = true
	}

	node := createNode(ulid)
	conf.Delegate = newObserveNodeMessage(opt, ready, msgCh, node)
	conf.Events = newObserveNodeEvent(opt, ready, evtCh)

	list, err := memberlist.Create(conf)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}
	if resolvLater {
		node.setPort(int(list.LocalNode().Port))
		opt.logger.SetPrefix(fmt.Sprintf("%s(%s:%d) ", node.ID(), node.Addr(), node.Port()))
	}

	b := newBully(ready, cancel, node, list, opt)
	b.wg.Add(2)
	go b.readNodeMessageLoop(ctx, msgCh, evtCh)
	go b.readNodeEventLoop(ctx, evtCh)

	if err := b.readyChannel(ctx); err != nil {
		cancel()
		return nil, errors.WithStack(err)
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
