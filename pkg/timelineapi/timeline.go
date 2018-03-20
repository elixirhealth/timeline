package timelineapi

import (
	"fmt"

	"github.com/pkg/errors"
)

const (
	// MaxLimit is the maximum value for the timeline limit on the number of events to return.
	MaxLimit = 32
)

var (
	// ErrEmptyUserID indicates when the request user ID is missing.
	ErrEmptyUserID = errors.New("missing user ID")

	// ErrEmptyTimeRange indicates when the request time range is missing.
	ErrEmptyTimeRange = errors.New("missing time range")

	// ErrLimitTooLarge indicates when the request limit is above the maximum value.
	ErrLimitTooLarge = fmt.Errorf("limit larger than maximum value %d", MaxLimit)
)

// ValidateGetRequest validates that the request has the required fields set.
func ValidateGetRequest(rq *GetRequest) error {
	if rq.UserId == "" {
		return ErrEmptyUserID
	}
	if rq.TimeRange == nil {
		return ErrEmptyTimeRange
	}
	if rq.Limit > MaxLimit {
		return ErrLimitTooLarge
	}
	return nil
}
