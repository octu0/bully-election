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
	CoordinatorMessage nodeMessageType = iota + 1
	TransferLeadershipMessage
	ReadyElectionMessage
	ReadySyncedMessage
)

func (m nodeMessageType) String() string {
	switch m {
	case CoordinatorMessage:
		return "msg<Coordinator>"
	case TransferLeadershipMessage:
		return "msg<TransferLeadership>"
	case ReadyElectionMessage:
		return "msg<ReadyElectionMessage>"
	case ReadySyncedMessage:
		return "msg<ReadySyncedMessage>"
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

func (b *Bully) sendCoordinatorMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, CoordinatorMessage)
}

func (b *Bully) sendTransferLeaderMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, TransferLeadershipMessage)
}

func (b *Bully) sendReadyElectionMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, ReadyElectionMessage)
}

func (b *Bully) sendReadySyncedMessage(targetNodeID string) error {
	return b.sendMessage(targetNodeID, ReadySyncedMessage)
}

type recvReadyElectionCount struct {
	Ch chan int
}

func (b *Bully) readNodeMessageLoop(ctx context.Context, ch chan []byte, evtCh chan *nodeEventMsg) {
	defer b.wg.Done()

	readyElectionCount := 0
	for {
		select {
		case <-ctx.Done():
			return

		case recv := <-b.recvReadyCount:
			recv.Ch <- readyElectionCount

		case data := <-ch:
			msg, err := unmarshalNodeMessage(bytes.NewReader(data))
			if err != nil {
				b.opt.onErrorFunc(errors.Wrapf(err, "recv data: %s", data))
				continue
			}

			switch msg.Type {
			case TransferLeadershipMessage:
				b.opt.logger.Printf("info: transfer_leadership message: %s", msg.NodeID)

				evtMsg := &nodeEventMsg{TransferLeadershipEvent, msg.NodeID, msg.NodeAddr}
				select {
				case evtCh <- evtMsg:
					// ok
				case <-time.After(b.opt.retryNodeEventTimeout):
					b.opt.logger.Printf("warn: evtCh maybe hangup(transfer_leadership), drop msg: %+v", msg)
				}

			case CoordinatorMessage:
				b.opt.logger.Printf("info: coordinator message: %s", msg.NodeID)
				readyElectionCount = 0 // reset

				b.setLeaderID(msg.NodeID)
				if err := b.syncState(stateRunning); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "coordinator message update: %s", msg.NodeID))
					continue
				}

			case ReadyElectionMessage:
				b.opt.logger.Printf("info: election_ready %s(%s)", msg.NodeID, msg.NodeAddr)
				readyElectionCount += 1

			case ReadySyncedMessage:
				b.opt.logger.Printf("info: election_synced %s(%s)", msg.NodeID, msg.NodeAddr)

				if err := b.syncState(stateElecting); err != nil {
					b.opt.onErrorFunc(errors.Wrapf(err, "state change : %s", stateElecting))
					continue
				}
			}
		}
	}
}
