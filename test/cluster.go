package test

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/vitalyisaev2/abd"
	"github.com/vitalyisaev2/abd/broadcast"
	"github.com/vitalyisaev2/abd/utils"
)

func init() {
	rand.Seed(time.Now().Unix())
}

// cluster is an abstraction for the group of the ABD processes;
// used for integration tests
type cluster interface {
	// getProcessIDs returns list of processes' IDs
	getProcessIDs() []utils.ProcessID
	// getProcessByID returns the process with the given ID
	getProcessByID(id utils.ProcessID) (abd.Process, error)
	// getRandomProcess returns arbitrary process
	getRandomProcess() abd.Process
	// quit terminates cluster
	quit()
}

type localhostCluster struct {
	processes []abd.Process
}

func (l localhostCluster) getProcessIDs() []utils.ProcessID {
	result := make([]utils.ProcessID, 0, len(l.processes))
	for _, p := range l.processes {
		result = append(result, p.ID())
	}

	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })

	return result
}

func (l localhostCluster) getProcessByID(id utils.ProcessID) (abd.Process, error) {
	for _, p := range l.processes {
		if p.ID() == id {
			return p, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("no process with id %v", id))
}

func (l localhostCluster) getRandomProcess() abd.Process {
	ix := rand.Intn(len(l.processes))
	return l.processes[ix]
}

func (l localhostCluster) quit() {
	for _, p := range l.processes {
		p.Quit()
	}
}

// newLocalhostCluster runs cluster of the ABD processes in the same address space (for the sake of integration testing)
func newLocalhostCluster(totalProcesses int) (cluster, error) {
	c := &localhostCluster{
		processes: make([]abd.Process, totalProcesses),
	}

	// prepare array of broadcasting objects
	broadcasts := make([]broadcast.Broadcast, totalProcesses)
	for i := 0; i < totalProcesses; i++ {
		broadcasts[i] = broadcast.NewBroadcast()
	}

	// launch the processes
	for i := 0; i < totalProcesses; i++ {
		processID := utils.ProcessID(i + 1)

		var err error
		c.processes[i], err = abd.NewProcess(processID, broadcasts[i])

		if err != nil {
			return nil, errors.Wrapf(err, "new process %v", processID)
		}
	}

	// register every process for every broadcasting object to make processes interact
	for i := 0; i < totalProcesses; i++ {
		for j := 0; j < totalProcesses; j++ {
			if i != j {
				if err := broadcasts[i].RegisterClient(c.processes[j]); err != nil {
					return nil, errors.Wrapf(err, "register client for the node %v", c.processes[j].ID())
				}
			}
		}
	}

	return c, nil
}
