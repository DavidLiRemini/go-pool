package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	//ErrMaxActiveConnReached 连接池超限
	ErrMaxActiveConnReached = errors.New("MaxActiveConnReached")
)

// Config 连接池相关配置
type Config struct {
	//连接池中拥有的最小连接数
	MinPoolSize int
	//最大并发存活连接数
	MaxPoolSize int
	//最大空闲连接
	MaxIdle int
	//生成连接的方法
	Factory func() (interface{}, error)
	//关闭连接的方法
	Close func(interface{}) error
	//检查连接是否有效的方法
	Ping func(interface{}) error
	//连接最大空闲时间，超过该事件则将失效
	IdleTimeout time.Duration
}

type connReq struct {
	idleConn *idleConn
}

// channelPool 存放连接信息
type channelPool struct {
	mu                       sync.RWMutex
	conns                    chan *idleConn
	factory                  func() (interface{}, error)
	close                    func(interface{}) error
	ping                     func(interface{}) error
	idleTimeout, waitTimeOut time.Duration
	maxActive                int
	minActive                int
	openingConns             int
	waitingCount             int
	waitingQueue             chan connReq
}

type idleConn struct {
	conn interface{}
	t    time.Time
}

// NewChannelPool 初始化连接
func NewChannelPool(poolConfig *Config) (Pool, error) {
	if !(poolConfig.MinPoolSize <= poolConfig.MaxIdle && poolConfig.MaxPoolSize >= poolConfig.MaxIdle && poolConfig.MinPoolSize >= 0) {
		return nil, errors.New("invalid capacity settings")
	}
	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}
	if poolConfig.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &channelPool{
		conns:        make(chan *idleConn, poolConfig.MaxIdle),
		factory:      poolConfig.Factory,
		close:        poolConfig.Close,
		idleTimeout:  poolConfig.IdleTimeout,
		maxActive:    poolConfig.MaxPoolSize,
		minActive:    poolConfig.MinPoolSize,
		openingConns: poolConfig.MinPoolSize,
		waitingQueue: make(chan connReq, 1024),
	}

	if poolConfig.Ping != nil {
		c.ping = poolConfig.Ping
	}

	for i := 0; i < poolConfig.MinPoolSize; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &idleConn{conn: conn, t: time.Now()}
	}

	return c, nil
}

// getConns 获取所有连接
func (c *channelPool) getConns() chan *idleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

func (c *channelPool) RegisterChecker(interval time.Duration, check func(interface{}) bool) {
	if interval > 0 && check != nil {
		go func() {
			for {
				time.Sleep(interval)
				if c.conns == nil {
					// pool aleardy destroyed, exit
					return
				}
				l := c.Len()
				for idx := 0; idx < l; idx++ {
					select {
					case i := <-c.conns:
						if timeout := c.idleTimeout; timeout > 0 {
							if i.t.Add(timeout).Before(time.Now()) {
								//丢弃并关闭该连接
								c.Close(i.conn)
								continue
							}
						}
						v := i.conn
						if !check(v) {
							c.Close(v)
							continue
						} else {
							select {
							case c.conns <- i:
								continue
							default:
								c.Close(v)
							}
						}
					default:
						break
					}
				}

				for {
					c.mu.Lock()
					conns := c.openingConns
					c.mu.Unlock()
					if conns < c.minActive {
						conn, err := c.Connect()
						if err == nil {
							select {
							case c.conns <- &idleConn{conn: conn, t: time.Now()}:
							default:
								break
							}
						}
						conns++
					} else {
						break
					}
				}
				log.Printf("pool size:%d", c.Len())
			}
		}()

	}
}

func (c *channelPool) GetContext(ctx context.Context) (interface{}, error) {
	if c.conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-c.conns:
			if wrapConn == nil {
				return nil, ErrClosed
			}
			// 额外检查一次是否context过期了，因为当多个条件满足时`select`可能会随机选择一个
			select {
			default:
			case <-ctx.Done():
				return wrapConn.conn, ctx.Err()
			}
			//判断是否超时，超时则丢弃
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					//丢弃并关闭该连接
					c.Close(wrapConn.conn)
					continue
				}
			}
			//判断是否失效，失效则丢弃，如果用户没有设定 ping 方法，就不检查
			if c.ping != nil {
				if err := c.Ping(wrapConn.conn); err != nil {
					c.Close(wrapConn.conn)
					continue
				}
			}
			return wrapConn.conn, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			c.mu.Lock()
			// FIXME 强制限制连接数逻辑先去掉
			//if c.openingConns >= c.maxActive {
			//	log.Printf("openingConns>MaxActive, openingConns:%d, maxActive:%d", c.openingConns, c.maxActive)
			//	c.waitingCount++
			//	c.mu.Unlock()
			//	var ret connReq
			//	select {
			//	case ret = <-c.waitingQueue:
			//	case <-ctx.Done():
			//		c.mu.Lock()
			//		c.waitingCount--
			//		c.mu.Unlock()
			//		return nil, ctx.Err()
			//	}
			//	//不返回错误，暂时注释掉
			//	//ret, ok := <-req
			//	//if !ok {
			//	//	return nil, ErrMaxActiveConnReached
			//	//}

			//	if timeout := c.idleTimeout; timeout > 0 {
			//		if ret.idleConn.t.Add(timeout).Before(time.Now()) {
			//			//丢弃并关闭该连接
			//			c.Close(ret.idleConn.conn)
			//			continue
			//		}
			//	}
			//	return ret.idleConn.conn, nil
			//}
			if c.factory == nil {
				c.mu.Unlock()
				return nil, ErrClosed
			}
			conn, err := c.factory()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			c.openingConns++
			c.mu.Unlock()
			return conn, nil
		}
	}
}

// Get 从pool中取一个连接
func (c *channelPool) Get() (interface{}, error) {
	return c.GetContext(context.Background())
}

func (c *channelPool) Connect() (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.factory == nil {
		return nil, ErrClosed
	}
	conn, err := c.factory()
	if err != nil {
		return nil, err
	}
	c.openingConns++
	return conn, nil
}

// Put 将连接放回pool中
func (c *channelPool) Put(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	if c.conns == nil {
		return c.Close(conn)
	}
	//if c.waitingCount > 0 {
	//	c.waitingQueue <- connReq{
	//		idleConn: &idleConn{conn: conn, t: time.Now()},
	//	}
	//	c.waitingCount--
	//	c.mu.Unlock()
	//	return nil
	//} else
	{
		select {
		case c.conns <- &idleConn{conn: conn, t: time.Now()}:
			return nil
		default:
			//连接池已满，直接关闭该连接
			return c.Close(conn)
		}
	}
}

// Close 关闭单条连接
func (c *channelPool) Close(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.close == nil {
		return nil
	}
	c.openingConns--
	return c.close(conn)
}

// Ping 检查单条连接是否有效
func (c *channelPool) Ping(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	return c.ping(conn)
}

// Release 释放连接池中所有连接
func (c *channelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.ping = nil
	closeFun := c.close
	c.close = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.conn)
	}
}

// Len 连接池中已有的连接
func (c *channelPool) Len() int {
	return len(c.getConns())
}
