package dcpool

import (
	"context"
	"errors"
	"sync"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/iyear/tdl/core/logctx"
	"github.com/iyear/tdl/core/middlewares/takeout"
)

var testMode = false

// ErrClosed reports use of a pool after Close has started.
var ErrClosed = errors.New("dcpool: pool is closed")

type closedInvoker struct{}

func (closedInvoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error { return ErrClosed }

// EnableTestMode enables test mode, which disables takeout and pooling and directly returns original client.
func EnableTestMode() {
	testMode = true
}

type Pool interface {
	Client(ctx context.Context, dc int) *tg.Client
	Takeout(ctx context.Context, dc int) *tg.Client
	Default(ctx context.Context) *tg.Client
	Close() error
}

type pool struct {
	api         *telegram.Client
	size        int64
	mu          sync.Mutex
	middlewares []telegram.Middleware

	invokers   map[int]tg.Invoker
	closes     map[int]func() error
	takeout    int64
	closed     bool
	closeDone  chan struct{}
	closeErr   error
	newInvoker func(context.Context, int) (telegram.CloseInvoker, error)
}

func NewPool(c *telegram.Client, size int64, middlewares ...telegram.Middleware) Pool {
	return &pool{
		api:         c,
		size:        size,
		middlewares: middlewares,
		invokers:    make(map[int]tg.Invoker),
		closes:      make(map[int]func() error),
		takeout:     0,
		closeDone:   make(chan struct{}),
	}
}

func (p *pool) current() int {
	return p.api.Config().ThisDC
}

func (p *pool) Client(ctx context.Context, dc int) *tg.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return tg.NewClient(closedInvoker{})
	}

	return tg.NewClient(p.invoker(ctx, dc))
}

func (p *pool) invoker(ctx context.Context, dc int) tg.Invoker {
	// self-hosted Telegram server can't properly handle pooling connections,
	// so directly return original client
	if testMode {
		return p.api
	}

	if i, ok := p.invokers[dc]; ok {
		return i
	}

	// lazy init
	var (
		invoker telegram.CloseInvoker
		err     error
	)
	if p.newInvoker != nil {
		invoker, err = p.newInvoker(ctx, dc)
	} else if dc == p.current() { // can't transfer dc to current dc
		invoker, err = p.api.Pool(p.size)
	} else {
		invoker, err = p.api.DC(ctx, dc, p.size)
	}

	if err != nil {
		logctx.From(ctx).Error("create invoker", zap.Error(err))
		return p.api // degraded
	}

	p.closes[dc] = invoker.Close
	p.invokers[dc] = chainMiddlewares(invoker, p.middlewares...)

	return p.invokers[dc]
}

func (p *pool) Default(ctx context.Context) *tg.Client {
	return p.Client(ctx, p.current())
}

func (p *pool) Close() (err error) {
	p.mu.Lock()
	if p.closed {
		done := p.closeDone
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		err = p.closeErr
		p.mu.Unlock()
		return err
	}
	// long: 生命周期锁先阻止新的 DC 初始化，再复制连接集合，关闭回调放到锁外执行。
	p.closed = true
	var takeoutInvoker tg.Invoker
	if p.takeout != 0 {
		base := p.invoker(context.TODO(), p.current())
		takeoutInvoker = chainMiddlewares(base, takeout.Middleware(p.takeout))
	}
	closes := make([]func() error, 0, len(p.closes))
	for _, closeFn := range p.closes {
		closes = append(closes, closeFn)
	}
	p.mu.Unlock()

	if takeoutInvoker != nil {
		err = takeout.UnTakeout(context.TODO(), takeoutInvoker)
	}
	for _, c := range closes {
		err = multierr.Append(err, c())
	}

	p.mu.Lock()
	p.closeErr = err
	close(p.closeDone)
	p.mu.Unlock()
	return err
}

func (p *pool) Takeout(ctx context.Context, dc int) *tg.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return tg.NewClient(closedInvoker{})
	}

	// lazy init
	if p.takeout == 0 {
		sid, err := takeout.Takeout(ctx, p.api)
		if err != nil {
			logctx.From(ctx).Warn("takeout error", zap.Error(err))
			// ignore init delay error and return non-takeout client
			// long: Client() 会再次获取 p.mu；在当前锁内直接复用创建逻辑，避免失败时自锁。
			return tg.NewClient(p.invoker(ctx, dc))
		}
		p.takeout = sid
		logctx.From(ctx).Info("get takeout id", zap.Int64("id", sid))
	}

	return tg.NewClient(chainMiddlewares(p.invoker(ctx, dc), takeout.Middleware(p.takeout)))
}
