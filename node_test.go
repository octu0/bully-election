package bullyelection

import (
	"bytes"
	"strings"
	"testing"
)

func TestNodeMeta(t *testing.T) {
	t.Run("toJSON/fromJSON", func(tt *testing.T) {
		sb := strings.Builder{}
		if err := toJSON(&sb, nodeMeta{
			ID:   "test1",
			Addr: "testaddr",
		}); err != nil {
			tt.Fatalf("toJSON: %+v", err)
		}
		nodeMeta, err := fromJSON(strings.NewReader(sb.String()))
		if err != nil {
			tt.Fatalf("fromJSON: %+v", err)
		}
		if nodeMeta.ID != "test1" {
			tt.Errorf("unserialize from json")
		}
		if nodeMeta.Addr != "testaddr" {
			tt.Errorf("unserialize from json")
		}
	})
	t.Run("metaToNode/full", func(tt *testing.T) {
		node := nodeMetaToNode(nodeMeta{
			ID:           "testid",
			Addr:         "testaddr",
			IsVoter:      true,
			State:        "teststate",
			UserMetadata: []byte("testusermetadata"),
			NumElection:  123,
			NumAnswer:    456,
			LeaderID:     "testid",
			NodeNumber:   7890,
		})
		if node.ID() != "testid" {
			tt.Errorf("ID")
		}
		if node.Addr() != "testaddr" {
			tt.Errorf("Addr")
		}
		if node.IsVoterNode() != true {
			tt.Errorf("IsVoterNode")
		}
		if bytes.Equal(node.UserMetadata(), []byte("testusermetadata")) != true {
			tt.Errorf("UserMetadata")
		}
		if node.IsLeader() != true {
			tt.Errorf("IsLeader")
		}
		vn := node.(internalVoterNode)
		if vn.getState() != "teststate" {
			tt.Errorf("State")
		}
		if vn.getNumElection() != 123 {
			tt.Errorf("getNumElection")
		}
		if vn.getNumAnswer() != 456 {
			tt.Errorf("getNumAnswer")
		}
		if vn.getNodeNumber() != 7890 {
			tt.Errorf("getNodeNumber")
		}
	})
	t.Run("metaToNode/partial", func(tt *testing.T) {
		node := nodeMetaToNode(nodeMeta{
			ID:      "testid",
			Addr:    "testaddr",
			IsVoter: true,
			State:   "teststate",
		})
		if node.ID() != "testid" {
			tt.Errorf("ID")
		}
		if node.Addr() != "testaddr" {
			tt.Errorf("Addr")
		}
		if node.IsVoterNode() != true {
			tt.Errorf("IsVoterNode")
		}
		if bytes.Equal(node.UserMetadata(), []byte{}) != true {
			tt.Errorf("initial value")
		}
		if node.IsLeader() {
			tt.Errorf("IsLeader")
		}
		vn := node.(internalVoterNode)
		if vn.getState() != "teststate" {
			tt.Errorf("State")
		}
		if vn.getNumElection() != 0 {
			tt.Errorf("initial value")
		}
		if vn.getNumAnswer() != 0 {
			tt.Errorf("initial value")
		}
		if vn.getNodeNumber() != 0 {
			tt.Errorf("initial value")
		}
	})
}
