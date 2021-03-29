package abd

import (
	"github.com/pkg/errors"
)

type Node struct {
	value     Value          // locally stored copy TODO: change to template when Go releases generics
	timestamp SequenceNumber // largest known timestamp
	t         SequenceNumber // sequence number of the writer
	r         SequenceNumber // sequence number of the reader
	broadcast Broadcast
}

func (n *Node) Write(val Value) error {
	if err := n.broadcast.Write(val, n.t); err != nil {
		return errors.Wrapf(err, "broadcast write value %v", val)
	}

	return nil
}

func (n *Node) Read() (Value, error) {
	results, err := n.broadcast.Read(n.r)
	if err != nil {
		return 0, errors.Wrapf(err, "broadcast read term=%v", n.r)
	}

	v := results.HighestTimestamp()
	if v.Timestamp > n.timestamp {
		n.value = v.Value
		n.timestamp = v.Timestamp
	}

	return n.value, nil
}
