package logger

import (
	"github.com/sirupsen/logrus"
	"net"
	"os"
)

type Logger *logrus.Entry

func CreateLogger(version, name string, level int32) Logger {
	_log := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.Level(level),
	}

	if version != "DEVELOPMENT" {
		_log.Formatter = &logrus.JSONFormatter{}
	}

	logger := _log.WithFields(logrus.Fields{
		"app":     name,
		"version": version,
	})

	// In production it'll be useful to have the pod name and it's IP address
	if version != "DEVELOPMENT" {
		hostname, err := os.Hostname()
		if err != nil {
			logger.WithError(err).Warnln("error getting hostname")
		} else {
			return logger.WithField("hostname", hostname)
		}

		ipAddrs, err := net.InterfaceAddrs()
		ips := make([]string, len(ipAddrs))
		for i, ip := range ipAddrs {
			ips[i] = ip.String()
		}
		if err != nil {
			logger.WithError(err).Warnln("error getting interface addrs")
		} else {
			return logger.WithField("ipAddrs", ips)
		}
	}

	return logger
}
