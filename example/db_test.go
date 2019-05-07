package example

import (
	"github.com/akikistyle/caplibgo/db"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_NewPool(t *testing.T) {
	tests := []struct {
		name          string
		wantErr       bool
		driver        string
		host          string
		port          int
		user          string
		pass          string
		database      string
		timeout       time.Duration
		maxIdle       int
		maxActive     int
		authorization bool
	}{
		{
			name:          "MongoDB",
			wantErr:       false,
			driver:        "mongodb",
			host:          "",
			port:          0,
			user:          "",
			pass:          "",
			database:      "",
			timeout:       1 * time.Hour,
			maxIdle:       500,
			authorization: true,
		},
		{
			name:          "Redis",
			wantErr:       false,
			driver:        "redis",
			host:          "",
			port:          0,
			database:      "",
			timeout:       240 * time.Second,
			maxIdle:       30,
			maxActive:     1000,
			authorization: false,
		}, {
			name:    "Connect error",
			wantErr: true,
			driver:  "mongodb",
			host:    "",
			port:    0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opt := &db.DBOpts{
				Host:          test.host,
				Port:          test.port,
				User:          test.user,
				Password:      test.pass,
				Database:      test.database,
				Timeout:       test.timeout,
				MaxIdle:       test.maxIdle,
				MaxActive:     test.maxActive,
				Authorization: test.authorization,
			}
			rsp, err := db.NewPool(test.driver, opt)
			if test.wantErr {
				assert.Error(t, err)
				assert.Nil(t, rsp)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, rsp)
			}
		})
	}
}
