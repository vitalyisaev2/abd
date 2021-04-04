package utils

// ReadResult tries to emulate algebraic data type for the result of register read operation.
type ReadResult struct {
	Err       error
	Value     Value
	Timestamp SequenceNumber
}

func (rr *ReadResult) clone() *ReadResult {
	return &ReadResult{
		Value:     rr.Value,
		Timestamp: rr.Timestamp,
		Err:       rr.Err,
	}
}

// ReadResults is a collection of ReadResults :).
type ReadResults []*ReadResult

// Succeeded return the number of successful responses.
func (rrs ReadResults) Succeeded() int {
	count := 0

	for _, res := range rrs {
		if res.Err == nil {
			count++
		}
	}

	return count
}

// LatestTimestamp looks for the value with a highest timestamp.
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
	if errList.Len() > len(rrs)-Quorum(len(rrs)) {
		return 0, 0, errList.Err()
	}

	// otherwise return the most recent value
	return target.Value, target.Timestamp, nil
}
