package abd

import (
	"context"

	"github.com/pkg/errors"
)

// broadcast is used for the communication within a group of ABD processes
type broadcast interface {
	// store - command to store value in cluster;
	// returns after response of the majority of processes
	store(ctx context.Context, value Value, t SequenceNumber) error
	// load - command to obtain value from cluster;
	// returns after response of the majority of processes
	load(ctx context.Context, r SequenceNumber) ReadResults
	// postponed initialization
	setLocalProcess(ownProcessID ProcessID, process Process)
}

// simple implementation of broadcast, when all of the ABD processes
// are in the address space of a single OS process; no networking is involved
type localhostBroadcast struct {
	processes map[ProcessID]Process
}

func (l localhostBroadcast) store(ctx context.Context, value Value, timestamp SequenceNumber) error {
	errChan := make(chan error, len(l.processes))

	// call other processes asynchronously
	for processID, process := range l.processes {
		go func(process Process) {
			err := process.receiveStore(ctx, value, timestamp)
			if err != nil {
				err = errors.Wrapf(err, "receive store processID=%v", processID) // FIXME
			}

			errChan <- err
		}(process)
	}

	// wait for the majority of answers
	var (
		errList   ErrorList
		succeeded = 0
	)

	for err := range errChan {
		if err != nil {
			errList.Add(err)
		} else {
			succeeded++
		}

		if succeeded == quorum(len(l.processes)) {
			return nil
		}
	}

	return errList.Err()
}

func (l localhostBroadcast) load(ctx context.Context, r SequenceNumber) ReadResults {
	resultChan := make(chan *ReadResult, len(l.processes))

	// call other processes asynchronously
	for _, process := range l.processes {
		go func(process Process) {
			resultChan <- process.receiveLoad(ctx)
		}(process)
	}

	// wait for the majority of answers
	results := make(ReadResults, 0, len(l.processes))
	for result := range resultChan {
		results = append(results, result)

		if results.succeeded() == quorum(len(l.processes)) {
			return results
		}
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
