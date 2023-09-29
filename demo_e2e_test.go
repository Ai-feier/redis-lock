package redis_lock

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestClient_TryLock_e2e(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	testCases := []struct {
		name string

		before func()
		after  func()

		// 输入
		key        string
		expiration time.Duration

		client *Client

		wantErr  error
		wantLock *Lock
	}{
		{
			name:       "locked",
			key:        "locked-key",
			expiration: time.Minute,
			before:     func() {},
			after: func() {
				// 验证一下, redis 上的数据
				res, err := rdb.Del(context.Background(), "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
			client: NewClient(rdb),
			wantLock: &Lock{
				key: "locked-key",
			},
		},

		{
			name:       "network error",
			key:        "network-key",
			expiration: time.Minute,
			wantErr:    errors.New("network error"),
		},

		{
			name: "failed to lock",
			key:  "failed-key",
			before: func() {
				res, err := rdb.Set(context.Background(), "failed-key", "123", time.Minute).Result()
				require.NoError(t, err)
				require.Equal(t, "OK", res)
			},
			after: func() {
				res, err := rdb.Get(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, "123", res)
			},
			expiration: time.Minute,
			wantErr:    ErrFailedToPreemptLock,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()

			l, err := tc.client.TryLock(context.Background(), tc.key, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.NotNil(t, l.client)
			assert.Equal(t, tc.wantLock.key, l.key)
			assert.NotEmpty(t, l.value)
			tc.after()
		})
	}
}
