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
