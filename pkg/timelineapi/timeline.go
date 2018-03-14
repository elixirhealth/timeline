package timelineapi

import "github.com/pkg/errors"

const (
	MaxLimit = 32
)

var (
	ErrEmptyUserID    = errors.New("missing user ID")
	ErrEmptyTimeRange = errors.New("missing time range")
)

func ValidateGetRequest(rq *GetRequest) error {
	if rq.UserId == "" {
		return ErrEmptyUserID
	}
	if rq.TimeRange == nil {
		return ErrEmptyTimeRange
	}
	return nil
}
