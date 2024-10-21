package bullyelection

import (
	"testing"
)

func TestUtilFunc(t *testing.T) {
	t.Run("isAllState", func(tt *testing.T) {
		nodes1 := []internalVoterNode{
			&voterNode{id: "1", state: StateRunning.String()},
			&voterNode{id: "2", state: StateRunning.String()},
			&voterNode{id: "3", state: StateAnswered.String()},
		}
		if isAllState(nodes1, StateAnswered) {
			tt.Errorf("two running, one answered")
		}
		nodes2 := []internalVoterNode{
			&voterNode{id: "1", state: StateAnswered.String()},
			&voterNode{id: "2", state: StateAnswered.String()},
			&voterNode{id: "3", state: StateAnswered.String()},
		}
		if isAllState(nodes2, StateAnswered) != true {
			tt.Errorf("all answered")
		}
	})
	t.Run("numState", func(tt *testing.T) {
		nodes1 := []internalVoterNode{
			&voterNode{id: "1", state: StateRunning.String()},
			&voterNode{id: "2", state: StateRunning.String()},
			&voterNode{id: "3", state: StateAnswered.String()},
		}
		if numState(nodes1, StateRunning) != 2 {
			tt.Errorf("numState")
		}
		if numState(nodes1, StateAnswered) != 1 {
			tt.Errorf("numState")
		}

		nodes2 := []internalVoterNode{
			&voterNode{id: "1", state: StateAnswered.String()},
			&voterNode{id: "2", state: StateAnswered.String()},
			&voterNode{id: "3", state: StateAnswered.String()},
		}
		if numState(nodes2, StateRunning) != 0 {
			tt.Errorf("numState")
		}
		if numState(nodes2, StateAnswered) != 3 {
			tt.Errorf("numState")
		}
	})
	t.Run("filterVoterNodes", func(tt *testing.T) {
		nodes1 := filterVoterNodes([]Node{
			&voterNode{id: "test1"},
			&nonvoterNode{id: "test2"},
			&nonvoterNode{id: "test3"},
		})
		if len(nodes1) != 1 {
			tt.Errorf("one voter")
		}
		if nodes1[0].ID() != "test1" {
			tt.Errorf("filterNodes")
		}

		nodes2 := filterVoterNodes([]Node{
			&voterNode{id: "test1"},
			&nonvoterNode{id: "test2"},
			&voterNode{id: "test3"},
		})
		if len(nodes2) != 2 {
			tt.Errorf("two voter")
		}
		if nodes2[0].ID() != "test1" {
			tt.Errorf("keep order")
		}
		if nodes2[1].ID() != "test3" {
			tt.Errorf("keep order")
		}
	})
}
