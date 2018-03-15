package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTimeline_ok(t *testing.T) {
	config := NewDefaultConfig()
	c, err := newTimeline(config)
	assert.Nil(t, err)
	assert.Equal(t, config, c.config)
	// TODO assert.NotEmpty on other elements of server struct
	//assert.NotEmpty(t, c.storer)
}

func TestNewTimeline_err(t *testing.T) {
	badConfigs := map[string]*Config{
	// TODO add bad config instances
	}
	for desc, badConfig := range badConfigs {
		c, err := newTimeline(badConfig)
		assert.NotNil(t, err, desc)
		assert.Nil(t, c)
	}
}
