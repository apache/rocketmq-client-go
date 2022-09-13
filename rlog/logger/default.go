package logger

import (
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
	"strings"
)

type defaultLogger struct {
	logger *logrus.Logger
}

func init() {
	r := &defaultLogger{
		logger: logrus.New(),
	}
	level := os.Getenv("ROCKETMQ_GO_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "debug":
		r.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		r.logger.SetLevel(logrus.WarnLevel)
	case "error":
		r.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		r.logger.SetLevel(logrus.FatalLevel)
	default:
		r.logger.SetLevel(logrus.InfoLevel)
	}
	rlog.SetLogger(r)
}

func (l *defaultLogger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Debug(msg)
}

func (l *defaultLogger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Info(msg)
}

func (l *defaultLogger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Warning(msg)
}

func (l *defaultLogger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Error(msg)
}

func (l *defaultLogger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Fatal(msg)
}

func (l *defaultLogger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.logger.SetLevel(logrus.FatalLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

type Config struct {
	OutputPath    string
	MaxFileSizeMB int
	MaxBackups    int
	MaxAges       int
	Compress      bool
	LocalTime     bool
}

func (c *Config) Logger() *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   filepath.ToSlash(c.OutputPath),
		MaxSize:    c.MaxFileSizeMB, // MB
		MaxBackups: c.MaxBackups,
		MaxAge:     c.MaxAges,  // days
		Compress:   c.Compress, // disabled by default
		LocalTime:  c.LocalTime,
	}
}

const defaultLogPath = "/tmp/rocketmq-client.log"

func defaultConfig() Config {
	return Config{
		OutputPath:    defaultLogPath,
		MaxFileSizeMB: 10,
		MaxBackups:    5,
		MaxAges:       3,
		Compress:      false,
		LocalTime:     true,
	}
}

func (l *defaultLogger) Config(conf Config) (err error) {
	l.logger.Out = conf.Logger()
	return
}

func (l *defaultLogger) OutputPath(path string) (err error) {
	config := defaultConfig()
	config.OutputPath = path

	l.logger.Out = config.Logger()
	return
}
