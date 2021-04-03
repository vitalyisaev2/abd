package abd

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/vitalyisaev2/abd/broadcast"
	"github.com/vitalyisaev2/abd/node"
	"github.com/vitalyisaev2/abd/utils"
)

// Register provides public interface for a register
type Register interface {
	// Write - write value
	Write(ctx context.Context, val utils.Value) error
	// Read - read value
	Read(ctx context.Context) (utils.Value, error)
}

// Process - the ABD algorithm participant
type Process interface {
	Register  // every process must server as a register for the external clients
	node.Node // every process must serve as a cluster member
	// ID - returns process' unique id
	ID() ProcessID
	// Quit terminates algorithm
	Quit()
}

// ProcessID - unique identifier of the process
type ProcessID int32

var _ Process = (*processImpl)(nil)

type processImpl struct {
	localValue     utils.Value          // locally stored copy TODO: change to template when Go implements generics
	localTimestamp utils.SequenceNumber // largest known timestamp
	t              utils.SequenceNumber // sequence number of the writer
	r              utils.SequenceNumber // sequence number of the reader
	broadcast      broadcast.Broadcast  // abstracts from the way of communication with other processes
	requestChan    chan interface{}     // request channel, also used for synchronization
	id             ProcessID            // globally unique ID
	wg             sync.WaitGroup       // used for graceful stop
	exitChan       chan struct{}        // termination channel
}

type writeRequest struct {
	ctx          context.Context
	val          utils.Value
	responseChan chan error
}

type readRequest struct {
	ctx          context.Context
	responseChan chan *readResponse
}

type readResponse struct {
	val utils.Value
	err error
}

type receiveStoreRequest struct {
	ctx          context.Context
	val          utils.Value
	timestamp    utils.SequenceNumber
	responseChan chan error
}

type receiveLoadRequest struct {
	ctx          context.Context
	responseChan chan *utils.ReadResult
}

func (p *processImpl) Write(ctx context.Context, val utils.Value) error {
	request := &writeRequest{
		ctx:          ctx,
		val:          val,
		responseChan: make(chan error, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return utils.ErrProcessIsTerminating
	}

	select {
	case err := <-request.responseChan:
		return err
	case <-p.exitChan:
		return utils.ErrProcessIsTerminating
	}
}

func (p *processImpl) Read(ctx context.Context) (utils.Value, error) {
	request := &readRequest{
		ctx:          ctx,
		responseChan: make(chan *readResponse, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return 0, utils.ErrProcessIsTerminating
	}

	select {
	case resp := <-request.responseChan:
		return resp.val, resp.err
	case <-p.exitChan:
		return 0, utils.ErrProcessIsTerminating
	}
}

func (p *processImpl) Store(ctx context.Context, val utils.Value, timestamp utils.SequenceNumber) error {
	request := &receiveStoreRequest{
		ctx:          ctx,
		val:          val,
		timestamp:    timestamp,
		responseChan: make(chan error, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return utils.ErrProcessIsTerminating
	}

	select {
	case err := <-request.responseChan:
		return err
	case <-p.exitChan:
		return utils.ErrProcessIsTerminating
	}
}

func (p *processImpl) Load(ctx context.Context) *utils.ReadResult {
	request := &receiveLoadRequest{
		ctx:          ctx,
		responseChan: make(chan *utils.ReadResult, 1),
	}

	select {
	case p.requestChan <- request:
	case <-p.exitChan:
		return &utils.ReadResult{Err: utils.ErrProcessIsTerminating}
	}

	select {
	case resp := <-request.responseChan:
		return resp
	case <-p.exitChan:
		return &utils.ReadResult{Err: utils.ErrProcessIsTerminating}
	}
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
		t.responseChan <- p.handleStore(t.val, t.timestamp)
	case *receiveLoadRequest:
		t.responseChan <- p.handleLoad()
	default:
		panic("unexpected message type")
	}
}

func (p *processImpl) handleWrite(ctx context.Context, val utils.Value) error {
	p.t++

	if err := p.broadcast.Store(ctx, val, p.t); err != nil {
		return errors.Wrapf(err, "Broadcast Store value=%v term=%v", val, p.t)
	}

	return nil
}

func (p *processImpl) handleRead(ctx context.Context) (utils.Value, error) {
	p.r++

	results := p.broadcast.Load(ctx, p.r)

	latestValue, latestTimestamp, err := results.LatestTimestamp()
	if err != nil {
		return 0, errors.Wrapf(err, "Broadcast Load term=%v", p.r)
	}

	if latestTimestamp > p.localTimestamp {
		p.localValue = latestValue
		p.localTimestamp = latestTimestamp
	}

	return p.localValue, nil
}

func (p *processImpl) handleStore(val utils.Value, timestamp utils.SequenceNumber) error {
	if timestamp > p.localTimestamp {
		p.localValue = val
		p.localTimestamp = timestamp
	}

	return nil
}

func (p *processImpl) handleLoad() *utils.ReadResult {
	return &utils.ReadResult{Timestamp: p.localTimestamp, Value: p.localValue}
}

func (p *processImpl) ID() ProcessID { return p.id }

func (p *processImpl) Quit() {
	close(p.exitChan)
	p.wg.Wait()
}

var _ node.Client = (*clientLocalNonblocking)(nil)

// clientLocalNonblocking makes it possible to perform private calls to the process (important for the broadcast implementation)
type clientLocalNonblocking struct {
	p *processImpl
}

func (c clientLocalNonblocking) Store(_ context.Context, val utils.Value, timestamp utils.SequenceNumber) error {
	if err := c.p.handleStore(val, timestamp); err != nil {
		return errors.Wrap(err, "local handle store")
	}

	return nil
}

func (c clientLocalNonblocking) Load(_ context.Context) *utils.ReadResult { return c.p.handleLoad() }
func (c clientLocalNonblocking) ID() ProcessID                            { return c.p.ID() }

func NewProcess(id ProcessID, bc broadcast.Broadcast) (Process, error) {
	p := &processImpl{
		localValue:     0,
		localTimestamp: 0,
		t:              0,
		r:              0,
		broadcast:      bc,
		requestChan:    make(chan interface{}), // no capacity: used for synchronization
		exitChan:       make(chan struct{}),
		id:             id,
		wg:             sync.WaitGroup{},
	}

	// register itself in the broadcast
	if err := p.broadcast.RegisterClient(&clientLocalNonblocking{p: p}); err != nil {
		return nil, errors.Wrap(err, "register client")
	}

	p.wg.Add(1)
	go p.loop()

	return p, nil
}
