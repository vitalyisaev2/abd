package utils

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorList is an error collector that can be used as a single error as well
type ErrorList []error

func (list *ErrorList) Len() int { return len(*list) }

func (list *ErrorList) Add(err error) { (*list) = append((*list), err) }

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

		return errors.New(buf.String())
	}
}

var ErrProcessIsTerminating = errors.New("process is terminating")
