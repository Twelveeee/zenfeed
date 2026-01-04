// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package llm

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/telemetry/log"
)

// RateLimiter 定义限流器接口
type RateLimiter interface {
	// Wait 等待直到可以执行请求,如果 context 被取消则返回错误
	Wait(ctx context.Context) error
	// TryAcquire 尝试获取一个令牌,如果成功返回 true,否则返回 false
	TryAcquire() bool
}

// noopRateLimiter 是一个无操作的限流器,用于没有限流需求的场景
type noopRateLimiter struct{}

func newNoopRateLimiter() RateLimiter {
	return &noopRateLimiter{}
}

func (n *noopRateLimiter) Wait(ctx context.Context) error {
	return nil
}

func (n *noopRateLimiter) TryAcquire() bool {
	return true
}

// tokenBucketRateLimiter 使用令牌桶算法实现的限流器
type tokenBucketRateLimiter struct {
	rpm      int           // 每分钟请求数限制
	interval time.Duration // 每个令牌的生成间隔
	tokens   chan struct{} // 令牌桶
	mu       sync.Mutex
	stopCh   chan struct{}
	stopped  bool
}

// newTokenBucketRateLimiter 创建一个基于令牌桶算法的限流器
// rpm: 每分钟请求数限制
func newTokenBucketRateLimiter(rpm int) RateLimiter {
	if rpm <= 0 {
		return newNoopRateLimiter()
	}

	// 计算每个令牌的生成间隔
	interval := time.Minute / time.Duration(rpm)

	// 令牌桶容量设置为 rpm,允许突发流量
	limiter := &tokenBucketRateLimiter{
		rpm:      rpm,
		interval: interval,
		tokens:   make(chan struct{}, rpm),
		stopCh:   make(chan struct{}),
	}

	// 初始化令牌桶,填满令牌
	for i := 0; i < rpm; i++ {
		limiter.tokens <- struct{}{}
	}

	// 启动令牌生成器
	go limiter.refillTokens()

	log.Info(context.Background(), "rate limiter created", "rpm", rpm, "interval", interval)

	return limiter
}

// refillTokens 定期向令牌桶中添加令牌
func (t *tokenBucketRateLimiter) refillTokens() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 尝试添加一个令牌,如果桶满了则丢弃
			select {
			case t.tokens <- struct{}{}:
			default:
				// 令牌桶已满,丢弃这个令牌
			}
		case <-t.stopCh:
			return
		}
	}
}

// Wait 等待直到可以执行请求
func (t *tokenBucketRateLimiter) Wait(ctx context.Context) error {
	start := time.Now()
	availableTokens := len(t.tokens)

	log.Debug(ctx, "rate limiter waiting", "rpm", t.rpm, "available_tokens", availableTokens)

	select {
	case <-t.tokens:
		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			log.Info(ctx, "rate limiter wait completed", "rpm", t.rpm, "wait_duration", elapsed)
		} else {
			log.Debug(ctx, "rate limiter passed immediately", "rpm", t.rpm)
		}
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "rate limiter wait cancelled")
	case <-t.stopCh:
		return errors.New("rate limiter stopped")
	}
}

// TryAcquire 尝试获取一个令牌
func (t *tokenBucketRateLimiter) TryAcquire() bool {
	select {
	case <-t.tokens:
		return true
	default:
		return false
	}
}

// Stop 停止限流器
func (t *tokenBucketRateLimiter) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.stopped {
		close(t.stopCh)
		t.stopped = true
	}
}

// NewRateLimiter 根据 RPM 配置创建限流器
func NewRateLimiter(rpm int) RateLimiter {
	return newTokenBucketRateLimiter(rpm)
}
