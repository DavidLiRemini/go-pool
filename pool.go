package pool

import (
	"context"
	"errors"
	"time"
)

// ErrClosed 连接池已经关闭Error
var ErrClosed = errors.New("pool is closed")

// Pool 基本方法
type Pool interface {
	Get() (interface{}, error)

	GetContext(context.Context) (interface{}, error)

	RegisterChecker(interval time.Duration, check func(interface{}) bool)

	Put(interface{}) error

	Close(interface{}) error

	Connect() (interface{}, error)

	Release()

	Len() int
}
