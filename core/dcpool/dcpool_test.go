package dcpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
)

type testInvoker struct{ closes *atomic.Int32 }

func (*testInvoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error { return nil }
func (i *testInvoker) Close() error                                         { i.closes.Add(1); return nil }

func TestTakeoutFailureFallsBackWithoutLockingPoolTwice(t *testing.T) {
	p := NewPool(telegram.NewClient(1, "test", telegram.Options{}), 1).(*pool)
	invoker := &testInvoker{}
	var created atomic.Int32
	p.newInvoker = func(context.Context, int) (telegram.CloseInvoker, error) { created.Add(1); return invoker, nil }
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go func() { p.Takeout(ctx, 2); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Takeout failure deadlocked while falling back to Client")
	}
	if created.Load() != 1 {
		t.Fatalf("fallback created %d invokers, want 1", created.Load())
	}
}

func TestCloseSerializesClientCreationAndIsIdempotent(t *testing.T) {
	p := NewPool(telegram.NewClient(1, "test", telegram.Options{}), 1).(*pool)
	var closeCalls atomic.Int32
	p.newInvoker = func(context.Context, int) (telegram.CloseInvoker, error) {
		time.Sleep(time.Millisecond)
		return &testInvoker{closes: &closeCalls}, nil
	}
	p.Client(context.Background(), 0)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for dc := 1; dc <= 32; dc++ {
		wg.Add(1)
		go func(dc int) { defer wg.Done(); <-start; p.Client(context.Background(), dc) }(dc)
	}
	closeDone := make(chan error, 1)
	go func() { <-start; closeDone <- p.Close() }()
	close(start)
	wg.Wait()
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	closed := p.closed
	n := len(p.closes)
	p.mu.Unlock()
	if !closed || closeCalls.Load() != int32(n) {
		t.Fatalf("closed=%v close callbacks=%d entries=%d", closed, closeCalls.Load(), n)
	}
	if err := p.Client(context.Background(), 99).Invoker().Invoke(context.Background(), nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close client error=%v", err)
	}
}
