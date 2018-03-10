// +build acceptance

package acceptance

import (
	"testing"
)

type parameters struct {
	// TODO
}

type state struct {
	// TODO
}

func TestAcceptance(t *testing.T) {
	params := &parameters{
	// TODO
	}
	st := setUp(params)

	tearDown(st)
}

func setUp(params *parameters) *state {
	// TODO
	return nil
}

func tearDown(st *state) {
	// TODO
}
