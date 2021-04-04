package node

import (
	"context"

	"github.com/vitalyisaev2/abd/utils"
)

// Node describes private interface of a cluster member.
type Node interface {
	// receiveStore - other process' request to Store value
	Store(ctx context.Context, val utils.Value, timestamp utils.SequenceNumber) error
	// receiveLoad - other process' request to Load value
	Load(ctx context.Context) *utils.ReadResult
}

// Client is a client for a (possibly remote) node.
type Client interface {
	Node
	ID() utils.ProcessID
}
