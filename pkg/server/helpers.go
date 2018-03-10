package server

import (
	"errors"

	bstorage "github.com/elxirhealth/service-base/pkg/server/storage"
	"github.com/elxirhealth/timeline/pkg/server/storage"
	"go.uber.org/zap"
)

var (
	// ErrInvalidStorageType indicates when a storage type is not expected.
	ErrInvalidStorageType = errors.New("invalid storage type")
)

func getStorer(config *Config, logger *zap.Logger) (storage.Storer, error) {
	switch config.Storage.Type {
	case bstorage.Memory:
		return nil, nil
	// TODO add case statemnts for different valid Storage types
	default:
		return nil, ErrInvalidStorageType
	}
}
