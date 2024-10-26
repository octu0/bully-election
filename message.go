package bullyelection

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	bufferPool = &sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
)

type nodeMessageType uint8

const (
	ElectionMessage nodeMessageType = iota + 1
	AnswerMessage
	CoordinatorMessage
	TransferLeadershipMessage
)

func (m nodeMessageType) String() string {
	switch m {
	case ElectionMessage:
		return "msg<Election>"
	case AnswerMessage:
		return "msg<Answer>"
	case CoordinatorMessage:
		return "msg<Coordinator>"
	case TransferLeadershipMessage:
		return "msg<TransferLeadership>"
	}
	return "unknown message"
}

type nodeMessage struct {
	Type     nodeMessageType `json:"type"`
	NodeID   string          `json:"node-id"`
	NodeAddr string          `json:"node-addr"`
}

func marshalNodeMessage(out io.Writer, msgType nodeMessageType, nodeID, addr string) error {
	if err := json.NewEncoder(out).Encode(nodeMessage{msgType, nodeID, addr}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func unmarshalNodeMessage(in io.Reader) (nodeMessage, error) {
	msg := nodeMessage{}
	if err := json.NewDecoder(in).Decode(&msg); err != nil {
		return nodeMessage{}, errors.WithStack(err)
	}
	return msg, nil
}

func (b *Bully) sendMessage(targetNodeID string, t nodeMessageType) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	if err := marshalNodeMessage(buf, t, b.node.ID(), b.fulladdress()); err != nil {
		return errors.WithStack(err)
	}
	return b.send(targetNodeID, buf.Bytes())
}

func (b *Bully) sendElectionMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, ElectionMessage)
}

func (b *Bully) sendAnswerMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, AnswerMessage)
}

func (b *Bully) sendCoordinatorMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, CoordinatorMessage)
}

func (b *Bully) sendTransferLeaderMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, TransferLeadershipMessage)
}

func (b *Bully) readNodeMessageLoop(ctx context.Context, ch chan []byte, evtCh chan *nodeEventMsg) {
	defer b.wg.Done()

	dumpNodes := func() {
		for _, n := range b.listNodes() {
			b.opt.logger.Printf("debug: ID:%s(%s:%d) isVoter:%v isLeader:%v", n.ID(), n.Addr(), n.Port(), n.IsVoter(), n.IsLeader())
		}
	}
	for {
		select {
		case <-ctx.Done():
			return

		case data := <-ch:
			msg, err := unmarshalNodeMessage(bytes.NewReader(data))
			if err != nil {
				b.opt.onErrorFunc(errors.Wrapf(err, "recv data: %s", data))
				continue
			}

			switch msg.Type {
			case ElectionMessage:
				b.opt.logger.Printf("info: election message: %s", msg.NodeID)
				if err := b.sendAnswerMessage(msg.NodeID); err != nil {
					// It will automatically reply, so there may be cases where it tries to send to a NodeID
					// that is not yet included in Members(), and it is ok to ignore it.
					if errors.Is(err, ErrNodeNotFound) != true {
						b.opt.onErrorFunc(errors.Wrapf(err, "send answer(%s)", msg.NodeID))
						continue
					}
				}

			case AnswerMessage:
				b.opt.logger.Printf("info: answer message: %s", msg.NodeID)
				b.mu.Lock()
				b.electionCancel()
				b.electionCancel = nopCancelFunc()
				b.mu.Unlock()
				b.clearLeaderID()
				if err := b.updateNode(); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "clear leader(%s)", msg.NodeID))
					continue
				}

			case CoordinatorMessage:
				b.opt.logger.Printf("info: coordinator message: %s", msg.NodeID)
				b.setLeaderID(msg.NodeID)
				if err := b.updateNode(); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "update leader(%s)", msg.NodeID))
					continue
				}
				dumpNodes()

				evtMsg := &nodeEventMsg{ElectionEvent, msg.NodeID, msg.NodeAddr, true}
				select {
				case evtCh <- evtMsg:
					// ok
				case <-time.After(b.opt.retryNodeEventTimeout):
					b.opt.logger.Printf("warn: evtCh maybe hangup(election), drop msg: %+v", evtMsg)
				}

				if b.waitElection != nil {
					select {
					case b.waitElection <- struct{}{}:
						// ok
					default:
						b.opt.logger.Printf("warn: waitElection maybe hangup or no reader, drop")
					}
				}

			case TransferLeadershipMessage:
				b.opt.logger.Printf("info: transfer_leadership message: %s", msg.NodeID)

				evtMsg := &nodeEventMsg{TransferLeadershipEvent, msg.NodeID, msg.NodeAddr, true}
				select {
				case evtCh <- evtMsg:
					// ok
				case <-time.After(b.opt.retryNodeEventTimeout):
					b.opt.logger.Printf("warn: evtCh maybe hangup(transfer_leadership), drop msg: %+v", msg)
				}
			}
		}
	}
}
