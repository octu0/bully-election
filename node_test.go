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
			LeaderID:     "testid",
			ULID:         "7890",
		})
		if node.ID() != "testid" {
			tt.Errorf("ID")
		}
		if node.Addr() != "testaddr" {
			tt.Errorf("Addr")
		}
		if node.IsVoter() != true {
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
		if vn.getULID() != "7890" {
			tt.Errorf("getULID")
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
		if node.IsVoter() != true {
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
		if vn.getULID() != "" {
			tt.Errorf("initial value")
		}
	})
}
