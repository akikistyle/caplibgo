package db

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
	"time"
)

type Redis struct {
	DB  *redis.Pool
	Opt *DBOpts
}

func (r *Redis) Connect() error {
	r.DB = &redis.Pool{
		MaxIdle:     r.Opt.MaxIdle,
		MaxActive:   r.Opt.MaxActive,
		IdleTimeout: r.Opt.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", r.DBSource())
			if err != nil {
				return nil, errors.Wrap(err, "Redis can not be connected")
			}

			if r.Opt.Authorization {
				if _, err := c.Do("AUTH", r.Opt.Password); err != nil {
					c.Close()
					return nil, errors.Wrap(err, "Redis can not be connected")
				}
			}

			if _, err := c.Do("SELECT", r.Opt.Database); err != nil {
				c.Close()
				return nil, errors.Wrap(err, "Redis can not be connected")
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("Ping")
			return err
		},
	}
	return nil
}

func (r *Redis) Close() {
	r.DB.Close()
}

func (r *Redis) Option() *DBOpts {
	return r.Opt
}

func (r *Redis) DBSource() string {
	return fmt.Sprintf("%v:%v", r.Opt.Host, r.Opt.Port)
}

func NewRedis(opt *DBOpts) (*Redis, error) {
	m := &Redis{Opt: opt}
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (r *Redis) Get(key string) ([]byte, error) {
	conn := r.DB.Get()
	defer conn.Close()
	return redis.Bytes(conn.Do("GET", key))
}

func (r *Redis) Set(key string, val string, exp int64) error {
	conn := r.DB.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, val)
	if exp > 0 {
		_, err = conn.Do("EXPIRE", key, exp)
	}
	return err
}

func (r *Redis) IsExist(key string) bool {
	conn := r.DB.Get()
	defer conn.Close()
	a, _ := conn.Do("EXISTS", key)
	i := a.(int64)
	if i > 0 {
		return true
	}
	return false
}

func (r *Redis) Delete(key string) error {
	conn := r.DB.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	return err
}

func (r *Redis) Incr(key string) (int64, error) {
	conn := r.DB.Get()
	defer conn.Close()
	rsp, err := redis.Int64(conn.Do("INCR", key))
	return rsp, err
}

func (r *Redis) Flush() error {
	conn := r.DB.Get()
	defer conn.Close()
	_, err := conn.Do("FLUSHDB")
	return err
}
