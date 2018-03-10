package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDefaultParameters(t *testing.T) {
	p := NewDefaultParameters()
	assert.NotNil(t, p)
	// TODO assert.NotEmpty on other params
}
