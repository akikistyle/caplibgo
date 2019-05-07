package db

import (
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

type MongoDB struct {
	DB  *mgo.Session
	Opt *DBOpts
}

func (p *MongoDB) Connect() error {
	var err error
	conStr := ""
	if p.Opt.Authorization {
		conStr = "mongodb://%v:%v@%v:%v/%v"
		conStr = fmt.Sprintf(conStr, p.Opt.User, p.Opt.Password, p.Opt.Host, p.Opt.Port, p.Opt.Database)
	} else {
		conStr = "mongodb://%v:%v/%v"
		conStr = fmt.Sprintf(conStr, p.Opt.Host, p.Opt.Port, p.Opt.Database)
	}
	p.DB, err = mgo.Dial(conStr)
	if err != nil {
		return errors.Wrap(err, "MongoDB can not be connected")
	}
	p.DB.SetPoolLimit(p.Opt.MaxIdle)
	p.DB.SetSocketTimeout(p.Opt.Timeout)
	return nil
}

func (p *MongoDB) Close() {
	p.DB.Close()
}

func (p *MongoDB) Option() *DBOpts {
	return p.Opt
}

func NewMongoDB(opt *DBOpts) (*MongoDB, error) {
	m := &MongoDB{Opt: opt}
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	return m, nil
}
