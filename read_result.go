package abd

type ReadResult struct {
	Timestamp SequenceNumber
	Value     Value
	Err       error
}

type ReadResults []*ReadResult

func (rrs ReadResults) HighestTimestamp() *ReadResult {
	target := &ReadResult{}

	for _, rr := range rrs {
		if rr.Timestamp > target.Timestamp {
			target = rr
		}
	}

	return target
}
