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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoopRateLimiter(t *testing.T) {
	limiter := newNoopRateLimiter()
	ctx := context.Background()

	// 无限制,应该立即返回
	err := limiter.Wait(ctx)
	assert.NoError(t, err)

	// TryAcquire 应该总是返回 true
	assert.True(t, limiter.TryAcquire())
}

func TestTokenBucketRateLimiter_Basic(t *testing.T) {
	// 创建一个每分钟 60 个请求的限流器 (即每秒 1 个请求)
	limiter := NewRateLimiter(60)
	ctx := context.Background()

	// 第一个请求应该立即成功
	start := time.Now()
	err := limiter.Wait(ctx)
	assert.NoError(t, err)
	assert.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestTokenBucketRateLimiter_RateLimit(t *testing.T) {
	// 创建一个每分钟 6 个请求的限流器 (即每 10 秒 1 个请求)
	rpm := 6
	limiter := NewRateLimiter(rpm)
	ctx := context.Background()

	// 快速消耗所有令牌
	for i := 0; i < rpm; i++ {
		assert.True(t, limiter.TryAcquire())
	}

	// 此时令牌桶应该为空,TryAcquire 应该返回 false
	assert.False(t, limiter.TryAcquire())

	// Wait 应该等待直到有新令牌
	start := time.Now()
	err := limiter.Wait(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)

	// 应该等待大约 10 秒 (60秒/6个请求)
	expectedWait := time.Minute / time.Duration(rpm)
	assert.Greater(t, elapsed, expectedWait-100*time.Millisecond)
	assert.Less(t, elapsed, expectedWait+500*time.Millisecond)
}

func TestTokenBucketRateLimiter_ContextCancellation(t *testing.T) {
	// 创建一个每分钟 1 个请求的限流器
	limiter := NewRateLimiter(1)

	// 消耗唯一的令牌
	assert.True(t, limiter.TryAcquire())

	// 创建一个会被取消的 context
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Wait 应该在 context 被取消时返回错误
	err := limiter.Wait(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rate limiter wait cancelled")
}

func TestTokenBucketRateLimiter_ZeroRPM(t *testing.T) {
	// RPM 为 0 应该返回 noop 限流器
	limiter := NewRateLimiter(0)
	ctx := context.Background()

	// 应该没有限制
	for i := 0; i < 100; i++ {
		err := limiter.Wait(ctx)
		assert.NoError(t, err)
	}
}

func TestTokenBucketRateLimiter_NegativeRPM(t *testing.T) {
	// 负数 RPM 应该返回 noop 限流器
	limiter := NewRateLimiter(-10)
	ctx := context.Background()

	// 应该没有限制
	for i := 0; i < 100; i++ {
		err := limiter.Wait(ctx)
		assert.NoError(t, err)
	}
}

func TestTokenBucketRateLimiter_Concurrent(t *testing.T) {
	// 创建一个每分钟 60 个请求的限流器
	rpm := 60
	limiter := NewRateLimiter(rpm)
	ctx := context.Background()

	// 并发请求
	concurrency := 10
	done := make(chan bool, concurrency)

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		go func() {
			err := limiter.Wait(ctx)
			assert.NoError(t, err)
			done <- true
		}()
	}

	// 等待所有请求完成
	for i := 0; i < concurrency; i++ {
		<-done
	}

	elapsed := time.Since(start)

	// 前 rpm 个请求应该立即完成,剩余的需要等待
	// 由于有 60 个初始令牌,前 60 个请求应该很快完成
	// 但我们只发送了 10 个请求,所以应该很快完成
	if concurrency <= rpm {
		assert.Less(t, elapsed, 1*time.Second)
	}
}
