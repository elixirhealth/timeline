package server

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	up := make(chan *Timeline, 1)
	wg1 := new(sync.WaitGroup)
	wg1.Add(1)
	go func(wg2 *sync.WaitGroup) {
		defer wg2.Done()
		err := Start(okConfig, up)
		assert.Nil(t, err)
	}(wg1)

	x := <-up
	assert.NotNil(t, x)

	x.StopServer()
	wg1.Wait()
}
