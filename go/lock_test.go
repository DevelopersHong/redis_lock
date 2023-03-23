package redis_lock

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v9"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"redis_lock/mocks"
	"testing"
	"time"
)

func TestClient_TryLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tests := []struct {
		name string

		mock func() redis.Cmdable

		key        string
		expiration time.Duration

		wantLock *Lock
		wantErr  error
	}{
		{
			name:       "locked",
			key:        "locked-key",
			expiration: time.Minute,

			mock: func() redis.Cmdable {
				res := mocks.NewMockCmdable(ctrl)
				res.EXPECT().SetNX(gomock.Any(), "locked-key", gomock.Any(), time.Minute).
					Return(redis.NewBoolResult(true, nil))
				return res
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
			mock: func() redis.Cmdable {
				res := mocks.NewMockCmdable(ctrl)
				res.EXPECT().SetNX(gomock.Any(), "network-key", gomock.Any(), time.Minute).
					Return(redis.NewBoolResult(false, errors.New("network error")))
				return res
			},
			wantErr: errors.New("network error"),
		},
		{
			// 模拟并发竞争失败
			name:       "failed",
			key:        "failed-key",
			expiration: time.Minute,
			mock: func() redis.Cmdable {
				res := mocks.NewMockCmdable(ctrl)
				res.EXPECT().SetNX(gomock.Any(), "failed-key", gomock.Any(), time.Minute).
					Return(redis.NewBoolResult(false, nil))
				return res
			},
			wantErr: ErrFailedToPreemptLock,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRedisCmd := tt.mock()
			client := NewClient(mockRedisCmd)
			l, err := client.TryLock(context.Background(), tt.key, tt.expiration)
			assert.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, mockRedisCmd, l.client)
			assert.Equal(t, tt.key, l.key)
			assert.Equal(t, tt.expiration, l.expiration)
			assert.NotEmpty(t, l.value)
		})
	}
}
