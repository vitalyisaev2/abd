package abd

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// Process - the ABD algorithm participant
type Process interface {
	// Write - client request to handleWrite message
	Write(ctx context.Context, val Value) error
	// Read - client request to handleRead message
	Read(ctx context.Context) (Value, error)
	// receiveStore - other process' request to store value
	receiveStore(ctx context.Context, val Value, timestamp SequenceNumber) error
	// receiveLoad - other process' request to load value
	receiveLoad(ctx context.Context) *ReadResult
	// Quit terminates algorithm
	Quit()
}

// ProcessID - unique identifier of the process
type ProcessID int32

var _ Process = (*processImpl)(nil)

type processImpl struct {
	localValue     Value            // locally stored copy TODO: change to template when Go implements generics
	localTimestamp SequenceNumber   // largest known timestamp
	t              SequenceNumber   // sequence number of the writer
	r              SequenceNumber   // sequence number of the reader
	broadcast      broadcast        // abstracts from the way of communication with other processes
	requestChan    chan interface{} // request channel, also used for synchronization
	exitChan       chan struct{}    // termination channel
	wg             sync.WaitGroup
}

type writeRequest struct {
	ctx          context.Context
	val          Value
	responseChan chan error
}

type readRequest struct {
	ctx          context.Context
	responseChan chan *readResponse
}

type readResponse struct {
	val Value
	err error
}

type receiveStoreRequest struct {
	ctx          context.Context
	val          Value
	timestamp    SequenceNumber
	responseChan chan error
}

type receiveLoadRequest struct {
	ctx          context.Context
	responseChan chan *ReadResult
}

func (p *processImpl) Write(ctx context.Context, val Value) error {
	request := &writeRequest{
		ctx:          ctx,
		val:          val,
		responseChan: make(chan error, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return ErrProcessIsTerminating
	}

	select {
	case err := <-request.responseChan:
		return err
	case <-p.exitChan:
		return ErrProcessIsTerminating
	}
}

func (p *processImpl) Read(ctx context.Context) (Value, error) {
	request := &readRequest{
		ctx:          ctx,
		responseChan: make(chan *readResponse, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return 0, ErrProcessIsTerminating
	}

	select {
	case resp := <-request.responseChan:
		return resp.val, resp.err
	case <-p.exitChan:
		return 0, ErrProcessIsTerminating
	}
}

func (p *processImpl) receiveStore(ctx context.Context, val Value, timestamp SequenceNumber) error {
	request := &receiveStoreRequest{
		ctx:          ctx,
		val:          val,
		timestamp:    timestamp,
		responseChan: make(chan error, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return ErrProcessIsTerminating
	}

	select {
	case err := <-request.responseChan:
		return err
	case <-p.exitChan:
		return ErrProcessIsTerminating
	}
}

func (p *processImpl) receiveLoad(ctx context.Context) *ReadResult {
	request := &receiveLoadRequest{
		ctx:          ctx,
		responseChan: make(chan *ReadResult, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return &ReadResult{Err: ErrProcessIsTerminating}
	}

	select {
	case resp := <-request.responseChan:
		return resp
	case <-p.exitChan:
		return &ReadResult{Err: ErrProcessIsTerminating}
	}
}

func (p *processImpl) Quit() {
	close(p.exitChan)
	p.wg.Wait()
}

func (p *processImpl) loop() {
	defer p.wg.Done()

	for {
		select {
		case request := <-p.requestChan:
			p.handleRequest(request)
		case <-p.exitChan:
			return
		}
	}
}

func (p *processImpl) handleRequest(request interface{}) {
	switch t := request.(type) {
	case *writeRequest:
		t.responseChan <- p.handleWrite(t.ctx, t.val)
	case *readRequest:
		val, err := p.handleRead(t.ctx)
		t.responseChan <- &readResponse{val: val, err: err}
	case *receiveStoreRequest:
		t.responseChan <- p.handleReceiveStore(t.val, t.timestamp)
	case *receiveLoadRequest:
		t.responseChan <- p.handleReceiveLoad()
	default:
		panic("unexpected message type")
	}
}

func (p *processImpl) handleWrite(ctx context.Context, val Value) error {
	p.t++

	if err := p.broadcast.store(ctx, val, p.t); err != nil {
		return errors.Wrapf(err, "broadcast store value=%v term=%v", val, p.t)
	}

	return nil
}

func (p *processImpl) handleRead(ctx context.Context) (Value, error) {
	p.r++

	results := p.broadcast.load(ctx, p.r)

	latestValue, latestTimestamp, err := results.LatestTimestamp()
	if err != nil {
		return 0, errors.Wrapf(err, "broadcast load term=%v", p.r)
	}

	if latestTimestamp > p.localTimestamp {
		p.localValue = latestValue
		p.localTimestamp = latestTimestamp
	}

	return p.localValue, nil
}

func (p *processImpl) handleReceiveStore(val Value, timestamp SequenceNumber) error {
	if timestamp > p.localTimestamp {
		p.localValue = val
		p.localTimestamp = timestamp
	}

	return nil
}

func (p *processImpl) handleReceiveLoad() *ReadResult {
	return &ReadResult{Timestamp: p.localTimestamp, Value: p.localValue}
}

func NewProcess(broadcast broadcast) Process {
	p := &processImpl{
		localValue:     0,
		localTimestamp: 0,
		t:              0,
		r:              0,
		broadcast:      broadcast,
		requestChan:    make(chan interface{}), // no capacity: used for synchronization
		exitChan:       make(chan struct{}),
		wg:             sync.WaitGroup{},
	}

	p.wg.Add(1)
	go p.loop()

	return p
}
