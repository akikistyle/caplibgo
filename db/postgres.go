package db

import (
	"fmt"
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
	"github.com/golang-migrate/migrate/source"
	"github.com/golang-migrate/migrate/source/go_bindata"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"log"
	"os"
	"time"
)

type PostgresDB struct {
	DB  *sqlx.DB
	Opt *DBOpts
}

func (p *PostgresDB) Connect() error {
	var err error
	p.DB, err = sqlx.Connect("postgres", p.DBSource())
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

func (p *PostgresDB) DBSource() string {
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable", p.Opt.User, p.Opt.Password, p.Opt.Host, p.Opt.Port, p.Opt.Database)
}

func NewPostgres(opt *DBOpts) (*PostgresDB, error) {
	m := &PostgresDB{Opt: opt}
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (p *PostgresDB) IsMigrationRequired(s source.Driver, mg *migrate.Migrate) (required bool, dirty bool, err error) {
	version, dirty, err := mg.Version()
	if err != nil {
		if err == migrate.ErrNilVersion {
			return true, false, nil
		}
		return false, false, errors.Wrap(err, "error getting current migration version")
	}

	next, err := s.Next(version)
	if os.IsNotExist(err) {
		// no up migrations exist for the current database version
		return false, dirty, nil
	}
	if err != nil {
		return false, dirty, errors.Wrap(err, "error getting next migration")
	}

	// failed migrations leave the dirty flag set. if the running commit has the
	// latest migration, the likelihood of the code not failing is... undetermined.
	// we'll want to exit early, let the deploy pause and continue running on old
	// code until it's fixed. old code doesn't have the latest migration, so it
	// won't think a migration is required
	required = (next > version) || (next == version && dirty)
	return required, dirty, nil
}

func (p *PostgresDB) prepareMigration(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) (source.Driver, *migrate.Migrate, error) {
	driver, err := postgres.WithInstance(p.DB.DB, &postgres.Config{MigrationsTable: conf.MigrationsItem, DatabaseName: conf.DatabaseName})
	if err != nil {
		return nil, nil, errors.Wrap(err, "error preparing postgres migration driver")
	}

	s, err := bindata.WithInstance(bindata.Resource(assets, afn))
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating source driver")
	}

	m, err := migrate.NewWithInstance("go-bindata", s, p.Opt.Database, driver)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating a new Migrate instance")
	}
	m.Log = conf.Logger

	return s, m, nil
}

func (p *PostgresDB) MigrateUpIfRequired(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error {
	defer p.DB.Close()

	s, m, err := p.prepareMigration(conf, assets, afn)
	if err != nil {
		return errors.Wrap(err, "error preparing migration")
	}
	defer m.Close()

	required, dirty, err := p.IsMigrationRequired(s, m)
	if err != nil {
		return errors.Wrap(err, "error checking if migration is required")
	}

	if required && dirty {
		return errors.New("migration required, but the database is dirty")
	}

	if !required {
		log.Println("database migration NOT required")
		return nil
	}

	log.Println("database migrations required, migrating...")
	retry := 0
	for {
		err = m.Up()
		if err == migrate.ErrNoChange {
			return nil
		}

		if err == migrate.ErrLocked || err == migrate.ErrLockTimeout {
			retry++
			if retry > 5 {
				return errors.Wrap(err, "error migrating database")
			}
			log.Printf("error obtaining lock,retry=%d,%v\n", retry, err)

			time.Sleep(time.Duration(retry) * time.Second)
		}
	}

	return nil
}

func (p *PostgresDB) MigrateUp(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error {
	_, m, err := p.prepareMigration(conf, assets, afn)
	if err != nil {
		return errors.Wrap(err, "error preparing migration")
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return errors.Wrap(err, "error migrating database")
	}

	return nil
}
