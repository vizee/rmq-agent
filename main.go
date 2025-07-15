package main

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

var app struct {
	ctx context.Context

	config *Config

	onExit func()
}

func addOnExit(f func()) {
	prev := app.onExit
	app.onExit = func() {
		f()
		if prev != nil {
			prev()
		}
	}
}

func initApp(ctx context.Context) {
	_, _ = maxprocs.Set(maxprocs.Logger(func(s string, i ...any) {
		_, _ = fmt.Printf(s+"\n", i...)
	}))
	app.ctx = ctx

	configFile := cmp.Or(os.Getenv("APP_CONFIG"), "./config.yaml")
	config, err := loadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v", err)
		os.Exit(1)
	}
	app.config = config

	initLog(config)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	initApp(ctx)

	logger.Info("app start")

	mainDone := make(chan struct{})
	go func() {
		err := runServer()
		if err != nil {
			logger.Error("run server", zap.Error(err))
		}
		close(mainDone)
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigs:
		logger.Info("app exiting")
	case <-mainDone:
	}

	cancel()
	if app.onExit != nil {
		app.onExit()
	}
	<-mainDone

	logger.Info("main exited")
}
