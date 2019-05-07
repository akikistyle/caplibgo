package db

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type PostgresDB struct {
	DB  *sqlx.DB
	Opt *DBOpts
}

func (p *PostgresDB) Connect() error {
	var err error
	conStr := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable", p.Opt.User, p.Opt.Password, p.Opt.Host, p.Opt.Port, p.Opt.Database)
	p.DB, err = sqlx.Connect("postgres", conStr)
	if err != nil {
		return errors.Wrap(err, "Postgres can not be connected")
	}
	return nil
}

func (p *PostgresDB) Close() {
	p.DB.Close()
}

func (p *PostgresDB) Option() *DBOpts {
	return p.Opt
}

func NewPostgres(opt *DBOpts) (*PostgresDB, error) {
	m := &PostgresDB{Opt: opt}
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	return m, nil
}
