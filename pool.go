package pool

import (
	"context"
	"errors"
)

var (
	//ErrClosed 连接池已经关闭Error
	ErrClosed = errors.New("pool is closed")
)

// Pool 基本方法
type Pool interface {
	Get() (interface{}, error)
	
	GetContext(context.Context)(interface{}, error)

	Put(interface{}) error

	Close(interface{}) error

	Connect() (interface{}, error)

	Release()

	Len() int
}
