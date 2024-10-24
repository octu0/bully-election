package bullyelection

import (
	"testing"
)

func TestUtilFunc(t *testing.T) {
	numState := func(nodes []internalVoterNode, targetState electionState) int {
		total := 0
		for _, n := range nodes {
			if n.getState() == targetState.String() {
				total += 1
			}
		}
		return total
	}
	t.Run("isAllState", func(tt *testing.T) {
		nodes1 := []internalVoterNode{
			&voterNode{id: "1", state: stateRunning.String()},
			&voterNode{id: "2", state: stateRunning.String()},
			&voterNode{id: "3", state: stateElecting.String()},
		}
		if isAllState(nodes1, stateElecting) {
			tt.Errorf("two running, one electing")
		}
		nodes2 := []internalVoterNode{
			&voterNode{id: "1", state: stateElecting.String()},
			&voterNode{id: "2", state: stateElecting.String()},
			&voterNode{id: "3", state: stateElecting.String()},
		}
		if isAllState(nodes2, stateElecting) != true {
			tt.Errorf("all electing")
		}
	})
	t.Run("numState", func(tt *testing.T) {
		nodes1 := []internalVoterNode{
			&voterNode{id: "1", state: stateRunning.String()},
			&voterNode{id: "2", state: stateRunning.String()},
			&voterNode{id: "3", state: stateElecting.String()},
		}
		if numState(nodes1, stateRunning) != 2 {
			tt.Errorf("numState")
		}
		if numState(nodes1, stateElecting) != 1 {
			tt.Errorf("numState")
		}

		nodes2 := []internalVoterNode{
			&voterNode{id: "1", state: stateElecting.String()},
			&voterNode{id: "2", state: stateElecting.String()},
			&voterNode{id: "3", state: stateElecting.String()},
		}
		if numState(nodes2, stateRunning) != 0 {
			tt.Errorf("numState")
		}
		if numState(nodes2, stateElecting) != 3 {
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
