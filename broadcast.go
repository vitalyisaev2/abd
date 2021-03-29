package abd

type Broadcast interface {
	Write(value Value, t SequenceNumber) error
	Read(r SequenceNumber) (ReadResults, error)
}

type ReadResult struct {
	Timestamp SequenceNumber
	Value     Value
}

type ReadResults []ReadResult

func (rrs ReadResults) HighestTimestamp() ReadResult {
	var target ReadResult

	for _, rr := range rrs {
		if rr.Timestamp > target.Timestamp {
			target = rr
		}
	}

	return target
}
