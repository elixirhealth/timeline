package timelineapi

import (
	"testing"
	"time"

	"github.com/magiconair/properties/assert"
)

func TestValidateGetRequest(t *testing.T) {
	now := time.Now().UnixNano() / 1E3
	prev := now - 1
	tr := &TimeRange{
		LowerBound: prev,
		UpperBound: now,
	}
	userID := "some user ID"
	cases := map[string]struct {
		rq       *GetRequest
		expected error
	}{
		"ok": {
			rq: &GetRequest{
				UserId:    userID,
				TimeRange: tr,
				Limit:     32,
			},
			expected: nil,
		},
		"zero limit ok": {
			rq: &GetRequest{
				UserId:    userID,
				TimeRange: tr,
			},
			expected: nil,
		},
		"missing user ID": {
			rq: &GetRequest{
				TimeRange: tr,
				Limit:     32,
			},
			expected: ErrEmptyUserID,
		},
		"missing time range": {
			rq: &GetRequest{
				UserId: userID,
				Limit:  32,
			},
			expected: ErrEmptyTimeRange,
		},
		"max limit too large": {
			rq: &GetRequest{
				UserId:    userID,
				TimeRange: tr,
				Limit:     33,
			},
			expected: ErrLimitTooLarge,
		},
	}

	for desc, c := range cases {
		err := ValidateGetRequest(c.rq)
		assert.Equal(t, c.expected, err, desc)
	}
}
