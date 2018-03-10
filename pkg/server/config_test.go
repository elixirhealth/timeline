package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDefaultConfig(t *testing.T) {
	c := NewDefaultConfig()
	assert.NotNil(t, c)
	// TODO assert certain config elements not empty
}

// TODO add TestConfig_WithCONFIGELEMENT functions for each CONFIGELEMENT
