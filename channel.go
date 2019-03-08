package pool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type PoolConfig struct {
	InitialCap int
	MaxActive  int
	MaxIdle    int

	Factory       func() (interface{}, error)
	Close         func(interface{}) error
	Ping          func(interface{}) error
	IdleTimeout   time.Duration
	CheckInterval time.Duration
}

type channelPool struct {
	mu            sync.Mutex
	conns         chan *idleConn
	maxActive     int
	maxIdle       int
	curConnCount  int
	factory       func() (interface{}, error)
	close         func(interface{}) error
	ping          func(interface{}) error
	idleTimeout   time.Duration
	checkInterval time.Duration
}

type idleConn struct {
	conn interface{}
	t    time.Time
}

//NewChannelPool init connect pool
func NewChannelPool(poolConfig *PoolConfig) (Pool, error) {
	if poolConfig.InitialCap < 0 || poolConfig.MaxActive <= 0 || poolConfig.InitialCap > poolConfig.MaxActive {
		return nil, errors.New("invalid capacity settings")
	}
	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}
	if poolConfig.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &channelPool{
		conns:         make(chan *idleConn, poolConfig.MaxActive),
		factory:       poolConfig.Factory,
		close:         poolConfig.Close,
		maxActive:     poolConfig.MaxActive,
		maxIdle:       poolConfig.MaxIdle,
		idleTimeout:   poolConfig.IdleTimeout,
		checkInterval: poolConfig.CheckInterval,
	}

	if poolConfig.Ping != nil {
		c.ping = poolConfig.Ping
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.curConnCount++
		c.conns <- &idleConn{conn: conn, t: time.Now()}
	}
	if c.checkInterval > 0 {
		go c.Check()
	}

	return c, nil
}

//getConns conn channel
func (c *channelPool) getConns() chan *idleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// cur counter --
func (c *channelPool) decrCurCount() {
	c.mu.Lock()
	c.curConnCount--
	c.mu.Unlock()
}

// cur counter ++
func (c *channelPool) incrCurCount() {
	c.mu.Lock()
	c.curConnCount++
	c.mu.Unlock()
}

func (c *channelPool) Check() {
	if c.idleTimeout == 0 {
		return
	}

	judgeTimeout := func() {
		if c.conns == nil {
			return
		}

		for {
			select {
			case wrapConn := <-c.conns:
				if wrapConn == nil {
					break
				}

				c.mu.Lock()
				if c.curConnCount > c.maxIdle {
					killStats := wrapConn.t.Add(c.idleTimeout).Before(time.Now())
					if killStats {
						c.Close(wrapConn.conn)
						c.curConnCount--
						c.mu.Unlock()
						continue
					}
				}
				c.mu.Unlock()
				c.conns <- wrapConn
			default:
				return
			}
		}
	}

	for {
		time.Sleep(c.checkInterval)
		judgeTimeout()
	}
}

//Get get conn in pool
func (c *channelPool) Get() (interface{}, error) {
	if c.conns == nil {
		return nil, ErrClosed
	}

	for {
		select {
		case wrapConn := <-c.conns:
			if wrapConn == nil {
				return nil, ErrClosed
			}
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					c.Close(wrapConn.conn)
					c.decrCurCount()
					continue
				}
			}

			if c.ping != nil {
				if err := c.Ping(wrapConn.conn); err != nil {
					continue
				}
			}
			return wrapConn.conn, nil

		default:
			c.incrCurCount()
			if c.curConnCount > c.maxActive {
				c.decrCurCount()

				time.Sleep(20 * time.Millisecond)
				continue
			}
			conn, err := c.factory()
			if err != nil {
				c.decrCurCount()
				return nil, err
			}

			return conn, nil
		}
	}
}

//Put put the connect to pool
func (c *channelPool) Put(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		c.curConnCount--
		return c.Close(conn)
	}

	select {
	case c.conns <- &idleConn{conn: conn, t: time.Now()}:
		return nil
	default:
		// connect pool is full, close the conn
		c.curConnCount--
		return c.Close(conn)
	}
}

//Close close a connect
func (c *channelPool) Close(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	return c.close(conn)
}

//Ping try check connect
func (c *channelPool) Ping(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	return c.ping(conn)
}

//Release release all conn in pool
func (c *channelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
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

//Len conns's count in pool
func (c *channelPool) Len() int {
	return len(c.getConns())
}

//GetCurCount all conns count, contains not return pool
func (c *channelPool) GetCurCount() int {
	return c.curConnCount
}
