package abd

type ReadResult struct {
	Value     Value
	Timestamp SequenceNumber
	Err       error
}

func (rr *ReadResult) clone() *ReadResult {
	return &ReadResult{
		Value:     rr.Value,
		Timestamp: rr.Timestamp,
		Err:       rr.Err,
	}
}

type ReadResults []*ReadResult

func (rrs ReadResults) succeeded() int {
	count := 0

	for _, res := range rrs {
		if res.Err == nil {
			count++
		}
	}

	return count
}

func (rrs ReadResults) LatestTimestamp() (Value, SequenceNumber, error) {
	var (
		target  = rrs[0].clone()
		errList ErrorList
	)

	for _, rr := range rrs {
		switch {
		case rr.Err != nil:
			errList.Add(rr.Err)
		case rr.Timestamp > target.Timestamp:
			target = rr
		}
	}

	// if there are too many errors, return error
	if errList.Len() > len(rrs)-quorum(len(rrs)) {
		return 0, 0, errList.Err()
	}

	// otherwise return the most recent value
	return target.Value, target.Timestamp, nil
}
