package redis_lock

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
	"time"
)

var (
	//go:embed script/lua/unlock.lua
	luaUnlock string
	//go:embed script/lua/refresh.lua
	luaRefresh string
	//go:embed script/lua/lock.lua
	luaLock string

	ErrFailedToPreemptLock = errors.New("rlock: 抢锁失败")
	// ErrLockNotHold 一般是出现在你预期你本来持有锁，结果却没有持有锁的地方
	// 比如说当你尝试释放锁的时候，可能得到这个错误
	// 这一般意味着有人绕开了 rlock 的控制，直接操作了 Redis
	ErrLockNotHold = errors.New("rlock: 未持有锁")
)

type Client struct {
	client redis.Cmdable // 客户可能传入单机redis, 或集群redis
	s      singleflight.Group
}

func NewClient(c redis.Cmdable) *Client {
	return &Client{
		client: c,
	}
}

func (c *Client) SingleflightLock(ctx context.Context,
	key string, expiration time.Duration, retry RetryStrategy, timeout time.Duration) (*Lock, error) {
	for {
		flag := false // 标记是否抢到
		resCh := c.s.DoChan(key, func() (interface{}, error) {
			flag = true
			return c.Lock(ctx, key, expiration, retry, timeout)
		})
		select {
		case res := <-resCh:
			// 判断是否是当前goroutine抢到锁
			if flag {
				c.s.Forget(key)
				if res.Err != nil {
					return nil, res.Err
				}
				return res.Val.(*Lock), nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Lock 是尽可能重试减少加锁失败的可能
// Lock 会在超时或者锁正被人持有的时候进行重试
// 最后返回的 error 使用 errors.Is 判断，可能是：
// - context.DeadlineExceeded: Lock 整体调用超时
// - ErrFailedToPreemptLock: 超过重试次数，但是整个重试过程都没有出现错误
// - DeadlineExceeded 和 ErrFailedToPreemptLock: 超过重试次数，但是最后一次重试超时了
// 你在使用的过程中，应该注意：
// - 如果 errors.Is(err, context.DeadlineExceeded) 那么最终有没有加锁成功，谁也不知道
// - 如果 errors.Is(err, ErrFailedToPreemptLock) 说明肯定没成功，而且超过了重试次数
// - 否则，和 Redis 通信出了问题
func (c *Client) Lock(ctx context.Context,
	key string, expiration time.Duration, retry RetryStrategy, timeout time.Duration) (*Lock, error) {
	value := uuid.New().String()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		lctx, cancelFunc := context.WithTimeout(ctx, timeout)
		res, err := c.client.Eval(lctx, luaLock, []string{key}, value).Result()
		cancelFunc()

		if !errors.Is(err, context.DeadlineExceeded) && err != nil {
			// 非超时错误，那么基本上代表遇到了一些不可挽回的场景，所以没太大必要继续尝试了
			// 比如说 Redis server 崩了，或者 EOF 了
			return nil, err
		}
		if res == "OK" {
			return NewLock(c.client, key, value, expiration), nil
		}
		// 加锁重试
		internal, ok := retry.Next()
		if !ok {
			if err != nil {
				err = fmt.Errorf("最后一次重试错误: %w", err)
			} else {
				err = fmt.Errorf("锁被人持有: %w", ErrFailedToPreemptLock)
			}
			// 不用重试, 达到最大强锁次数
			return nil, fmt.Errorf("rlock: 重试机会耗尽，%w", err)
		}

		if timer == nil {
			timer = time.NewTimer(internal) // 超时暂停
		} else {
			timer.Reset(internal) // 重置定时器
		}
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *Client) TryLock(ctx context.Context,
	key string, expiration time.Duration) (*Lock, error) {

	value := uuid.New().String()
	res, err := c.client.SetNX(ctx, key, value, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !res {
		return nil, ErrFailedToPreemptLock
	}
	return NewLock(c.client, key, value, expiration), nil
}

type Lock struct {
	client     redis.Cmdable
	key        string
	value      string
	expiration time.Duration
	unLocked   chan struct{}
}

func NewLock(client redis.Cmdable, key string, value string, expiration time.Duration) *Lock {
	return &Lock{
		client:     client,
		key:        key,
		value:      value,
		expiration: expiration,
		unLocked:   make(chan struct{}, 1),
	}
}

func (l *Lock) Unlock(ctx context.Context) error {
	defer func() {
		l.unLocked <- struct{}{}
		close(l.unLocked) // unlock()只预计允许调用一次
	}()
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.value).Result()
	if err == redis.Nil {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	// 判断 res
	if res == 0 {
		// 这把锁不是你的
		return ErrLockNotHold
	}

	// 解锁时, 需要确保当前锁是我的锁, 然后执行删除
	//result, err2 := l.client.Get(ctx, l.key).Result()
	//_, err := l.client.Del(ctx, l.key).Result()
	//if err != nil {
	//	return err
	//}
	//if res != 1 {
	//	// 1. 过期
	//	// 2. 被人删除
	//}
	return nil
}

func (l *Lock) AutoRefresh(internal, timeout time.Duration) error {
	ticker := time.NewTicker(internal) // 多久续约一次
	ch := make(chan struct{}, 1)
	defer func() {
		close(ch)
	}()
	for {
		select {
		case <-ch:
			ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(ctx)
			cancelFunc()
			if err == context.DeadlineExceeded {
				// 续约超时, 进行重试
				ch <- struct{}{}
				continue
			}
			if err != nil {
				return err
			}
		case <-ticker.C:
			ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(ctx)
			cancelFunc()
			if err == context.DeadlineExceeded {
				// 续约超时, 进行重试
				ch <- struct{}{}
				continue
			}
			if err != nil {
				return err
			}
		case <-l.unLocked:
			return nil
		}

	}

}

func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.value, l.expiration).Int64()
	if err == redis.Nil {
		// 锁是否是当前客户端的
		return ErrLockNotHold
	}
	if err != nil {
		// 服务出错
		return err
	}
	if res != 1 {
		// 续约不成功
		return ErrLockNotHold
	}
	return nil
}
