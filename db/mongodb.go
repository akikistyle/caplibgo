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

func (m *MongoDB) Connect() error {
	var err error
	m.DB, err = mgo.Dial(m.DBSource())
	if err != nil {
		return errors.Wrap(err, "MongoDB can not be connected")
	}
	m.DB.SetPoolLimit(m.Opt.MaxIdle)
	m.DB.SetSocketTimeout(m.Opt.Timeout)
	return nil
}

func (m *MongoDB) Close() {
	m.DB.Close()
}

func (m *MongoDB) Option() *DBOpts {
	return m.Opt
}

func (m *MongoDB) DBSource() string {
	if m.Opt.Authorization {
		return fmt.Sprintf("mongodb://%v:%v@%v:%v/%v", m.Opt.User, m.Opt.Password, m.Opt.Host, m.Opt.Port, m.Opt.Database)
	}
	return fmt.Sprintf("mongodb://%v:%v/%v", m.Opt.Host, m.Opt.Port, m.Opt.Database)
}

func NewMongoDB(opt *DBOpts) (*MongoDB, error) {
	m := &MongoDB{Opt: opt}
	err := m.Connect()
	if err != nil {
		return nil, err
	}
	return m, nil
}

//func (m *MongoDB) prepareMigration(ctx context.Context, conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) (*mongo.Client, source.Driver, *migrate.Migrate, error) {
//	opt := options.Client().ApplyURI(m.DBSource())
//	client, err := mongo.Connect(ctx, opt)
//	if err != nil {
//		return nil, nil, nil, errors.Wrap(err, "error preparing mongodb migration driver")
//	}
//
//	err = client.Connect(ctx)
//	if err != nil {
//		return nil, nil, nil, errors.Wrap(err, "error preparing mongodb migration driver")
//	}
//
//	driver, err := mongodb.WithInstance(client, &mongodb.Config{DatabaseName: conf.DatabaseName, MigrationsCollection: conf.MigrationsItem})
//	if err != nil {
//		return nil, nil, nil, errors.Wrap(err, "error preparing mongodb migration driver")
//	}
//
//	s, err := bindata.WithInstance(bindata.Resource(assets, afn))
//	if err != nil {
//		return nil, nil, nil, errors.Wrap(err, "error creating source driver")
//	}
//
//	mg, err := migrate.NewWithInstance("go-bindata", s, m.Opt.Database, driver)
//	if err != nil {
//		return nil, nil, nil, errors.Wrap(err, "error creating a new Migrate instance")
//	}
//	mg.Log = conf.Logger
//
//	return client, s, mg, nil
//}

//
//func (m *MongoDB) IsMigrationRequired(s source.Driver, mg *migrate.Migrate) (required bool, dirty bool, err error) {
//	version, dirty, err := mg.Version()
//	if err != nil {
//		if err == migrate.ErrNilVersion {
//			return true, false, nil
//		}
//		return false, false, errors.Wrap(err, "error getting current migration version")
//	}
//
//	next, err := s.Next(version)
//	if os.IsNotExist(err) {
//		// no up migrations exist for the current database version
//		return false, dirty, nil
//	}
//	if err != nil {
//		return false, dirty, errors.Wrap(err, "error getting next migration")
//	}
//
//	required = (next > version) || (next == version && dirty)
//	return required, dirty, nil
//}

//func (m *MongoDB) MigrateUpIfRequired(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error {
//	ctx, _ := context.WithTimeout(context.Background(), 1*time.Hour)
//	client, s, mg, err := m.prepareMigration(ctx, conf, assets, afn)
//	if err != nil {
//		return errors.Wrap(err, "error preparing migration")
//	}
//	defer func() {
//		client.Disconnect(ctx)
//	}()
//
//	required, dirty, err := m.IsMigrationRequired(s, mg)
//	if err != nil {
//		return errors.Wrap(err, "error checking if migration is required")
//	}
//
//	if required && dirty {
//		return errors.New("migration required, but the database is dirty")
//	}
//
//	if !required {
//		logrus.Infoln("database migration NOT required")
//		return nil
//	}
//
//	logrus.Infoln("database migrations required, migrating...")
//	retry := 0
//	for {
//		err = mg.Up()
//		if err == migrate.ErrNoChange {
//			return nil
//		}
//
//		if err == migrate.ErrLocked || err == migrate.ErrLockTimeout {
//			retry++
//			if retry > 5 {
//				return errors.Wrap(err, "error migrating database")
//			}
//			logrus.WithField("retry", retry).Warnln("error obtaining lock")
//
//			time.Sleep(time.Duration(retry) * time.Second)
//		}
//	}
//
//	return nil
//}
