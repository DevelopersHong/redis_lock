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
		Password: "",
		DB:       0,
	})
	client := NewClient(rdb)
	client.Wait()
	tests := []struct {
		name string

		before func()
		after  func()

		key        string
		expiration time.Duration

		wantLock *Lock
		wantErr  error
	}{
		{
			name:       "locked",
			key:        "locked-key",
			expiration: time.Minute,
			before: func() {

			},
			after: func() {
				res, err := rdb.Del(context.Background(), "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)

			},
			wantLock: &Lock{
				key:        "locked-key",
				expiration: time.Minute,
			},
		},
		{
			// mock 网络错误
			name:       "network error",
			key:        "network-key",
			expiration: time.Minute,
			wantErr:    errors.New("network error"),
		},
		{
			// 模拟并发竞争失败
			name:       "failed",
			key:        "failed-key",
			expiration: time.Minute,

			wantErr: ErrFailedToPreemptLock,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before()
			l, err := client.TryLock(context.Background(), tt.key, tt.expiration)
			assert.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}
			tt.after()
			assert.Equal(t, rdb, l.client)
			assert.Equal(t, tt.key, l.key)
			assert.Equal(t, tt.expiration, l.expiration)
			assert.NotEmpty(t, l.value)
		})
	}
}

func (c *Client) Wait() {
	for c.client.Ping(context.Background()) != nil {

	}
}
