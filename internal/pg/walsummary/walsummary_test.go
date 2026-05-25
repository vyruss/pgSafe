package walsummary_test

import (
	"errors"
	"testing"

	"github.com/vyruss/pgsafe/internal/pg/walsummary"
)

func TestNewRejectsPre17(t *testing.T) {
	t.Parallel()
	for _, v := range []int{13, 14, 15, 16} {
		// We only test version-gating here; nil pool is fine because the
		// version check runs first.
		_, err := walsummary.New(nil, v)
		if !errors.Is(err, walsummary.ErrUnsupported) {
			t.Errorf("New(_, %d): want ErrUnsupported, got %v", v, err)
		}
	}
}

func TestNewRequiresPool(t *testing.T) {
	t.Parallel()
	_, err := walsummary.New(nil, 18)
	if err == nil {
		t.Fatal("New(nil, 18): want error, got nil")
	}
	if errors.Is(err, walsummary.ErrUnsupported) {
		t.Errorf("New(nil, 18): want pool-required error, got ErrUnsupported")
	}
}
