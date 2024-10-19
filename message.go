package bullyelection

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

type MessageType uint8

const (
	ElectionMessage MessageType = iota + 1
	AnswerMessage
	CoordinatorMessage
)

type Message struct {
	Type   MessageType `json:"type"`
	NodeID string      `json:"node-id"`
}

func marshalMessage(out io.Writer, msgType MessageType, nodeID string) error {
	if err := json.NewEncoder(out).Encode(Message{msgType, nodeID}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func unmarshalMessage(in io.Reader) (Message, error) {
	msg := Message{}
	if err := json.NewDecoder(in).Decode(&msg); err != nil {
		return Message{}, errors.WithStack(err)
	}
	return msg, nil
}
