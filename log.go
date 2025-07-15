package main

import (
	"os"

	rmqclient "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger, _ = zap.NewDevelopment()
)

func initLog(config *Config) {
	logLevel, err := zap.ParseAtomicLevel(config.Log.Level)
	if err != nil {
		logLevel = zap.NewAtomicLevel()
	}
	if config.Log.Mode == "production" {
		w := zapcore.AddSync(&lumberjack.Logger{
			Filename: config.Log.Path,
			MaxSize:  int(config.Log.MaxSize),
			MaxAge:   config.Log.MaxAge,
		})
		ec := zap.NewProductionEncoderConfig()
		ec.TimeKey = "time"
		ec.EncodeTime = zapcore.RFC3339TimeEncoder
		encoder := zapcore.NewJSONEncoder(ec)
		logger = zap.New(zapcore.NewCore(encoder, w, logLevel))
	} else {
		cfg := zap.NewDevelopmentConfig()
		cfg.OutputPaths = []string{"stdout"}
		cfg.Level = logLevel
		logger, _ = cfg.Build()
	}

	if config.Rmq.Log != nil {
		os.Setenv(rmqclient.CLIENT_LOG_ROOT, config.Rmq.Log.Root)
		os.Setenv(rmqclient.CLIENT_LOG_FILENAME, config.Rmq.Log.FileName)
		os.Setenv(rmqclient.CLIENT_LOG_LEVEL, config.Rmq.Log.Level)
		rmqclient.ResetLogger()
	}
}
