package abd

import "context"

// broadcast is used for the communication of the ABD processes
type broadcast interface {
	// store - command to store value in cluster;
	// returns after response of the majority of processes
	store(ctx context.Context, value Value, t SequenceNumber) error
	// load - command to obtain value from cluster;
	// returns after response of the majority of processes
	load(ctx context.Context, r SequenceNumber) (ReadResults, error)
	// postponed initialization
	setLocalProcess(ownProcessID ProcessID, process Process)
}

// simple implementation of broadcast, when all of the ABD processes
// are in the address space of a single OS process; no networking is involved
type localhostBroadcast struct {
	processes map[ProcessID]Process
}

func (l localhostBroadcast) store(ctx context.Context, value Value, t SequenceNumber) error {
	panic("implement me")
}

func (l localhostBroadcast) load(ctx context.Context, r SequenceNumber) (ReadResults, error) {
	resultChan := make(chan *ReadResult, len(l.processes))

	for _, process := range l.processes {
		go func(process Process) {
			resultChan <- process.receiveLoad(ctx)
		}(process)
	}

	results := make(ReadResults, 0, len(l.processes))
	for result := range resultChan {
		results = append(results, result)
	}

	return results
}

func (l *localhostBroadcast) setLocalProcess(ownProcessID ProcessID, process Process) {
	_, exists := l.processes[ownProcessID]
	if exists {
		panic("implementation error: process already registered")
	}

	l.processes[ownProcessID] = process
}
