package broadcast

import (
	"context"

	"github.com/pkg/errors"
	"github.com/vitalyisaev2/abd/node"
	"github.com/vitalyisaev2/abd/utils"
)

// Broadcast is used for the communication within a group of ABD clients.
type Broadcast interface {
	// Store - command to store value in cluster;
	// returns after response of the majority of clients
	Store(ctx context.Context, value utils.Value, t utils.SequenceNumber) error
	// Load - command to obtain value from cluster;
	// returns after response of the majority of clients
	Load(ctx context.Context, r utils.SequenceNumber) utils.ReadResults
	// postponed initialization
	RegisterClient(client node.Client) error
}

// simple implementation of Broadcast, when all of the ABD clients
// are in the address space of a single OS process; no networking is involved.
type broadcastImpl struct {
	clients []node.Client
}

func (b broadcastImpl) Store(ctx context.Context, value utils.Value, timestamp utils.SequenceNumber) error {
	errChan := make(chan error, len(b.clients))

	// call other clients asynchronously
	for _, process := range b.clients {
		go func(client node.Client) {
			err := client.Store(ctx, value, timestamp)
			if err != nil {
				err = errors.Wrapf(err, "receive Store processID=%v", client.ID()) // FIXME
			}

			errChan <- err
		}(process)
	}

	// wait for the majority of answers
	var (
		errList   utils.ErrorList
		succeeded = 0
	)

	for err := range errChan {
		if err != nil {
			errList.Add(err)
		} else {
			succeeded++
		}

		if succeeded == utils.Quorum(len(b.clients)) {
			return nil
		}
	}

	return errList.Err()
}

func (b broadcastImpl) Load(ctx context.Context, r utils.SequenceNumber) utils.ReadResults {
	resultChan := make(chan *utils.ReadResult, len(b.clients))

	// call other clients asynchronously
	for _, client := range b.clients {
		go func(client node.Client) {
			resultChan <- client.Load(ctx)
		}(client)
	}

	// wait for the majority of answers
	results := make(utils.ReadResults, 0, len(b.clients))
	for result := range resultChan {
		results = append(results, result)

		if results.Succeeded() == utils.Quorum(len(b.clients)) {
			return results
		}
	}

	return results
}

func (b *broadcastImpl) RegisterClient(newClient node.Client) error {
	for _, existingClient := range b.clients {
		if existingClient.ID() == newClient.ID() {
			return errors.New("client to node %d is already registered")
		}
	}

	b.clients = append(b.clients, newClient)

	return nil
}

// NewBroadcast is a constructor for a Broadcast.
func NewBroadcast() Broadcast {
	return &broadcastImpl{}
}
