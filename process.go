package abd

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// Process - the ABD algorithm participant
type Process interface {
	// Write - client request to write message
	Write(ctx context.Context, val Value) error
	// Read - client request to read message
	Read(ctx context.Context) (Value, error)
	// ReceiveStore - other model request request to store value
	ReceiveStore(ctx context.Context, val Value) error
	// ReceiveLoad - other model deliver request to load value
	ReceiveLoad(ctx context.Context) (SequenceNumber, Value, error)
	// Quit terminates algorithm
	Quit()
}

var _ Process = (*processImpl)(nil)

type processImpl struct {
	localValue     Value          // locally stored copy TODO: change to template when Go implements generics
	localTimestamp SequenceNumber // largest known localTimestamp
	t              SequenceNumber // sequence number of the writer
	r              SequenceNumber // sequence number of the reader
	broadcast      Broadcast
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
	responseChan chan *receiveLoadResponse
}

type receiveLoadResponse struct {
	timestamp SequenceNumber
	val       Value
	err       error
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

func (p *processImpl) ReceiveStore(ctx context.Context, val Value) error {
	request := &receiveStoreRequest{
		ctx:          ctx,
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

func (p *processImpl) ReceiveLoad(ctx context.Context) (SequenceNumber, Value, error) {
	request := &receiveLoadRequest{
		ctx:          ctx,
		responseChan: make(chan *receiveLoadResponse, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return 0, 0, ErrProcessIsTerminating
	}

	select {
	case resp := <-request.responseChan:
		return resp.timestamp, resp.val, resp.err
	case <-p.exitChan:
		return 0, 0, ErrProcessIsTerminating
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
		t.responseChan <- p.write(t.ctx, t.val)
	case *readRequest:
		val, err := p.read(t.ctx)
		t.responseChan <- &readResponse{val: val, err: err}
	case *receiveStoreRequest:
		t.responseChan <- p.receiveStore(t.val, t.timestamp)
	case *receiveLoadRequest:
		t.responseChan <- p.receiveLoad()
	default:
		panic("unexpected message type")
	}
}

func (p *processImpl) write(ctx context.Context, val Value) error {
	p.t++

	if err := p.broadcast.Write(ctx, val, p.t); err != nil {
		return errors.Wrapf(err, "broadcast write value %v", val)
	}

	return nil
}

func (p *processImpl) read(ctx context.Context) (Value, error) {
	p.r++

	results, err := p.broadcast.Read(ctx, p.r)
	if err != nil {
		return 0, errors.Wrapf(err, "broadcast read term=%v", p.r)
	}

	v := results.HighestTimestamp()
	if v.Timestamp > p.localTimestamp {
		p.localValue = v.Value
		p.localTimestamp = v.Timestamp
	}

	return p.localValue, nil
}

func (p *processImpl) receiveStore(val Value, timestamp SequenceNumber) error {
	if timestamp > p.localTimestamp {
		p.localValue = val
		p.localTimestamp = timestamp
	}

	return nil
}

func (p *processImpl) receiveLoad() *receiveLoadResponse {
	return &receiveLoadResponse{timestamp: p.localTimestamp, val: p.localValue}
}

func NewProcess(broadcast Broadcast) Process {
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
