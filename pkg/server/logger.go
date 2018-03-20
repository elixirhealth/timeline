package server

import (
	"encoding/hex"

	api "github.com/elxirhealth/catalog/pkg/catalogapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logUserID        = "user_id"
	logNEntities     = "n_entities"
	logEntityID      = "entity_id"
	logNPubs         = "n_publications"
	logEnvKeyShort   = "envelope_key_short"
	logNEnvelopes    = "n_envelopes"
	logEntryKeyShort = "entry_key_short"
	logNEntries      = "n_entries"

	logCourier   = "courier"
	logCatalog   = "catalog"
	logDirectory = "directory"
	logUser      = "user"
)

func logEntityIDGet(userID string, entityIDs []string) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logUserID, userID),
		zap.Int(logNEntities, len(entityIDs)),
	}
}

func logPubsGet(entityID string, prs []*api.PublicationReceipt) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Int(logNPubs, len(prs)),
	}
}

func logAllPubsGet(entityIDs []string, prs []*api.PublicationReceipt) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNEntities, len(entityIDs)),
		zap.Int(logNPubs, len(prs)),
	}
}

func shortHex(val []byte) string {
	return hex.EncodeToString(val[:8])
}
