package db

import (
	"errors"
	"time"
)

type DBManager interface {
	Connect() error
	Close()
	Option() *DBOpts
	DBSource() string
}

type DBOpts struct {
	Host          string
	Port          int
	User          string
	Password      string
	Database      string
	Timeout       time.Duration
	MaxIdle       int
	MaxActive     int
	Authorization bool
}

func NewPool(driver string, opt *DBOpts) (DBManager, error) {
	switch driver {
	case "mongodb":
		return NewMongoDB(opt)
	case "redis":
		return NewRedis(opt)
	case "postgres":
		return NewPostgres(opt)
	case "mysql":
		return NewMysql(opt)
	}
	return nil, errors.New("can not find this driver type")
}
