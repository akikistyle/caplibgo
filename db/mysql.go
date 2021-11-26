package db

import (
	"fmt"
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/mysql"
	"github.com/golang-migrate/migrate/source"
	"github.com/golang-migrate/migrate/source/go_bindata"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"time"
)

type MysqlDB struct {
	DB  *sqlx.DB
	Opt *DBOpts
}

func (m *MysqlDB) Connect() error {
	var err error

	m.DB, err = sqlx.Open("mysql", m.DBSource())
	if err != nil {
		return errors.Wrap(err, "Postgres connection cannot be opened")
	}
	const maxRetries = 6
	for i := 1; i <= maxRetries; i++ {
		if err = m.DB.Ping(); err != nil {
			logrus.WithError(err).WithField("attempt", i).Warnln("Mysql not yet pinging")
			if i < maxRetries {
				// don't sleep on the last failure
				time.Sleep(time.Duration(i) * time.Second)
			}
			continue
		}
		//logrus.Println("connected to Postgres")
		if m.Opt.MaxActive > 0 {
			m.DB.SetMaxOpenConns(m.Opt.MaxActive)
		}
		if m.Opt.MaxIdle > 0 {
			m.DB.SetMaxIdleConns(m.Opt.MaxIdle)
		}

		return nil
	}

	return nil
}

func (m *MysqlDB) Close() {
	m.DB.Close()
}

func (m *MysqlDB) Option() *DBOpts {
	return m.Opt
}

func (m *MysqlDB) DBSource() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8", m.Opt.User, m.Opt.Password, m.Opt.Host, m.Opt.Port, m.Opt.Database)
}

func NewMysql(opt *DBOpts) (*MysqlDB, error) {
	m := &MysqlDB{Opt: opt}
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MysqlDB) IsMigrationRequired(s source.Driver, mg *migrate.Migrate) (required bool, dirty bool, err error) {
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

func (m *MysqlDB) prepareMigration(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) (source.Driver, *migrate.Migrate, error) {
	driver, err := mysql.WithInstance(m.DB.DB, &mysql.Config{MigrationsTable: conf.MigrationsItem, DatabaseName: conf.DatabaseName})
	if err != nil {
		return nil, nil, errors.Wrap(err, "error preparing mysql migration driver")
	}

	s, err := bindata.WithInstance(bindata.Resource(assets, afn))
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating source driver")
	}

	mi, err := migrate.NewWithInstance("go-bindata", s, m.Opt.Database, driver)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating a new Migrate instance")
	}
	mi.Log = conf.Logger

	return s, mi, nil
}

func (m *MysqlDB) MigrateUpIfRequired(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error {
	defer m.DB.Close()

	s, mi, err := m.prepareMigration(conf, assets, afn)
	if err != nil {
		return errors.Wrap(err, "error preparing migration")
	}
	defer m.Close()

	required, dirty, err := m.IsMigrationRequired(s, mi)
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
		err = mi.Up()
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

func (m *MysqlDB) MigrateUp(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error {
	_, mi, err := m.prepareMigration(conf, assets, afn)
	if err != nil {
		return errors.Wrap(err, "error preparing migration")
	}
	defer mi.Close()

	if err := mi.Up(); err != nil && err != migrate.ErrNoChange {
		return errors.Wrap(err, "error migrating database")
	}

	return nil
}
