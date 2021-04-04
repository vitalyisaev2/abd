package utils

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorList is an error collector that can be used as a single error as well.
type ErrorList []error

// Len returns the number of errors collected so far.
func (list *ErrorList) Len() int { return len(*list) }

// Add adds an error to the internal storage.
func (list *ErrorList) Add(err error) { (*list) = append((*list), err) }

// Err returns the final error.
func (list *ErrorList) Err() error {
	switch len(*list) {
	case 0:
		return nil
	case 1:
		return (*list)[0]
	default:
		buf := &strings.Builder{}

		for i, err := range *list {
			buf.WriteString(fmt.Sprintf("%d: %v", i, err))
		}

		// nolint: goerr113 // don't know how to avoid buffering
		return errors.New(buf.String())
	}
}

// ErrProcessIsTerminating tells the client that process is being terminated.
var ErrProcessIsTerminating = errors.New("process is terminating")
