package linearizability

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vitalyisaev2/abd/utils"
)

type eventKind int8

const (
	read eventKind = iota + 1
	write
)

func (ek eventKind) String() string {
	switch ek {
	case read:
		return "read"
	case write:
		return "write"
	default:
		panic("unknown event kind")
	}
}

type event struct {
	kind      eventKind
	value     utils.Value
	timestamp time.Time
}

func (e event) String() string {
	return fmt.Sprintf("kind=%v value=%v timestamp=%v", e.kind, e.value, e.timestamp)
}

// History is a collection of read/write events with timestamps
// TODO: this implementation is pessimistic, consider wait-free slice append operation
type History struct {
	mutex  sync.Mutex
	events []*event
}

func (h *History) RegisterRead(val utils.Value) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.events = append(h.events, &event{
		kind:      read,
		value:     val,
		timestamp: time.Now(),
	})
}

func (h *History) RegisterWrite(val utils.Value) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.events = append(h.events, &event{
		kind:      write,
		value:     val,
		timestamp: time.Now(),
	})
}

func (h *History) Analyze(t *testing.T) {
	// Definition from the Guerraoui, Kuznetsov, 2018:
	//
	// A history H is linearizable (with respect to the sequential specification of a read-
	// 	write register) if all its complete operations and a subset of incomplete operations
	// can be ordered in a sequential history S such that:
	// • In S every read in returns the last written value, and
	// • If the response of operation op 1 precedes the invocation of operation op 2 in
	// H, and we write op 1 < H op 2 , then op 1 < S op 2 .

	h.mutex.Lock()
	defer h.mutex.Unlock()

	// actually it's not necessary in this implementation, but let it stay
	sort.Slice(h.events, func(i, j int) bool {
		return h.events[i].timestamp.Before(h.events[j].timestamp)
	})

	// here we check only the first part of the definition;
	// TODO: implement second part, inject the history into register implementation
	var lastWriteEvent *event

	for _, ev := range h.events {
		switch ev.kind {
		case read:
			if lastWriteEvent == nil {
				// if read preceeds the first write, it must return 0
				require.Equal(t, utils.Value(0), ev.value)
			} else {
				require.Equal(t, lastWriteEvent.value, ev.value, fmt.Sprintf("\nlast=%v\ncurr=%v", lastWriteEvent, ev))
			}
		case write:
			if lastWriteEvent == nil {
				lastWriteEvent = ev
			}
		}
	}
}
