# pool

common connnect pool, difference with [fatih/pool](github.com/fatih/pool)

* More adaptation
* Add MaxIdle threshold
* ActiveCheck()
* CurConnCount Lock

some code from https://github.com/silenceper/pool

## Usage

```
factory := func() (interface{}, error) { return net.Dial("tcp", "127.0.0.1:4000") }

close := func(v interface{}) error { return v.(net.Conn).Close() }

//ping := func(v interface{}) error { return nil }

poolConfig := &pool.PoolConfig{
	InitialCap: 5,
	MaxActive:  200,
	MaxIdle:    50,
	Factory:    factory,
	Close:      close,

	IdleTimeout:   30 * time.Second,
	CheckInterval: 10 * time.Second,
}
p, err := pool.NewChannelPool(poolConfig)
if err != nil {
	fmt.Println("err=", err)
}

v, err := p.Get()

//conn=v.(net.Conn)

p.Put(v)

p.Release()

current := p.Len()
```
