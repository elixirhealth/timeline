package storage

import (
	bstorage "github.com/elxirhealth/service-base/pkg/server/storage"
	"go.uber.org/zap/zapcore"
)

var (
	// DefaultType is the default storage type.
	DefaultType = bstorage.Memory
)

// Storer ... TODO add rest of description.
type Storer interface {
	// TODO add methods
}

// Parameters defines the parameters of the Storer.
type Parameters struct {
	Type bstorage.Type

	// TODO add other params, often things like query timeouts to backend bstorage
}

// NewDefaultParameters returns a *Parameters object with default values.
func NewDefaultParameters() *Parameters {
	return &Parameters{
		Type: DefaultType,

		// TODO add other params defaults
	}
}

// MarshalLogObject writes the parameters to the given object encoder.
func (p *Parameters) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddString(logType, p.Type.String())
	// TODO log other params here
	return nil
}

// TODO (maybe) add other things common to all bstorage types here
