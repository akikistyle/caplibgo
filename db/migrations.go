package db

import (
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/source"
	"github.com/golang-migrate/migrate/source/go_bindata"
)

type DBMigrations interface {
	IsMigrationRequired(s source.Driver, mg *migrate.Migrate) (required bool, dirty bool, err error)
	MigrateUpIfRequired(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error
	MigrateUp(conf *DBMigrationsConfig, assets []string, afn bindata.AssetFunc) error
}

type DBMigrationsConfig struct {
	DatabaseName   string
	MigrationsItem string //Table or Collection
	Logger         migrate.Logger
}
