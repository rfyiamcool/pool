package pool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// PoolConfig 连接池相关配置
type PoolConfig struct {
	//连接池中拥有的最小连接数
	InitialCap int

	//连接池中拥有的最大的连接数
	MaxActive int
	MaxIdle   int

	//生成连接的方法
	Factory func() (interface{}, error)

	//关闭连接的方法
	Close func(interface{}) error

	//检查连接是否有效的方法
	Ping func(interface{}) error

	//连接最大空闲时间，超过该事件则将失效
	IdleTimeout time.Duration

	// 主动检测周期
	CheckInterval time.Duration
}

//channelPool 存放连接信息
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

//NewChannelPool 初始化连接
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

//getConns 获取所有连接
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

//Get 从pool中取一个连接
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
			//判断是否超时，超时则丢弃
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					//丢弃并关闭该连接
					c.Close(wrapConn.conn)
					c.decrCurCount()
					continue
				}
			}
			//判断是否失效，失效则丢弃，如果用户没有设定 ping 方法，就不检查
			if c.ping != nil {
				if err := c.Ping(wrapConn.conn); err != nil {
					fmt.Println("conn is not able to be connected: ", err)
					continue
				}
			}
			return wrapConn.conn, nil

		default:
			c.incrCurCount()
			if c.curConnCount > c.maxActive {
				c.decrCurCount()

				time.Sleep(20 * time.Millisecond)
				fmt.Println("conn pool full, sleep 20 ms")
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

//Put 将连接放回pool中
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
		//连接池已满，直接关闭该连接
		c.curConnCount--
		return c.Close(conn)
	}
}

//Close 关闭单条连接
func (c *channelPool) Close(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	return c.close(conn)
}

//Ping 检查单条连接是否有效
func (c *channelPool) Ping(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	return c.ping(conn)
}

//Release 释放连接池中所有连接
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

//Len 连接池中已有的连接
func (c *channelPool) Len() int {
	return len(c.getConns())
}

// 当前的连接数, 包含没有归还的
func (c *channelPool) GetCurCount() int {
	return c.curConnCount
}

