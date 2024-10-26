package bullyelection

import (
	"testing"
)

func TestFilterVoterNode(t *testing.T) {
	t.Run("nonvoter_only", func(tt *testing.T) {
		nodes := []Node{
			&nonvoterNode{id: "test1"},
			&nonvoterNode{id: "test2"},
			&nonvoterNode{id: "test3"},
		}
		if a := filterVoterNode(nodes); len(a) != 0 {
			tt.Errorf("nonvoter_only")
		}
	})
	t.Run("voter_only", func(tt *testing.T) {
		nodes := []Node{
			&voterNode{id: "test1"},
			&voterNode{id: "test2"},
			&voterNode{id: "test3"},
		}
		if a := filterVoterNode(nodes); len(a) != 3 {
			tt.Errorf("voter_only")
		}
	})
	t.Run("mix", func(tt *testing.T) {
		nodes := []Node{
			&voterNode{id: "test1"},
			&nonvoterNode{id: "test2"},
			&voterNode{id: "test3"},
		}
		if a := filterVoterNode(nodes); len(a) != 2 {
			tt.Errorf("mix")
		}
	})
}
