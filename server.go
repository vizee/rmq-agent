package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type Server struct {
	down      atomic.Bool
	cwg       *sync.WaitGroup
	consumers []Consumer
	producers map[string]*Producer
}

func (s *Server) stop() {
	for _, c := range s.consumers {
		err := c.Stop()
		if err != nil {
			logger.Warn("stop consumer", zap.String("name", c.Name()), zap.Error(err))
		}
	}
	s.cwg.Wait()

	for name, p := range s.producers {
		err := p.Stop()
		if err != nil {
			logger.Warn("stop producer", zap.String("name", name), zap.Error(err))
		}
	}
}

func (s *Server) start() error {
	for _, consumerConfig := range app.config.Rmq.Consumers {
		switch consumerConfig.ConsumeType {
		case "pull":
			c, err := newPullConsumer(consumerConfig, app.config.Rmq)
			if err != nil {
				return err
			}
			s.consumers = append(s.consumers, c)
		default:
			return fmt.Errorf("unsupported consumeType: %s", consumerConfig.ConsumeType)
		}
	}

	s.producers = make(map[string]*Producer, len(app.config.Rmq.Producers))
	for _, producerConfig := range app.config.Rmq.Producers {
		p, err := newProducer(producerConfig, app.config.Rmq)
		if err != nil {
			return err
		}
		s.producers[producerConfig.Name] = p
	}
	return nil
}

func (s *Server) handlePostRmqMessage(w http.ResponseWriter, r *http.Request) {
	req, err := readJSONRequest[SendMessageRequest](r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("SendMessage",
			zap.String("producer", req.Producer),
			zap.String("trxId", req.TrxId),
			zap.String("topic", req.Topic),
			zap.Int("body", len(req.Body)),
			zap.String("tag", req.Tag),
			zap.String("messageGroup", req.MessageGroup),
			zap.Strings("keys", req.Keys),
			zap.Timep("delayTime", req.DelayTime))
	}

	p := s.producers[req.Producer]
	if p == nil {
		http.Error(w, fmt.Sprintf("no such producer: %s", req.Producer), http.StatusNotFound)
		return
	}
	resp, err := p.SendMessage(req)
	if err != nil {
		logger.Error("SendMessage", zap.String("producer", req.Producer), zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, resp)
}

func (s *Server) handlePostRmqTrxBegin(w http.ResponseWriter, r *http.Request) {
	req, err := readJSONRequest[BeginTrxRequest](r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("BeginTrx", zap.String("producer", req.Producer))
	}

	p := s.producers[req.Producer]
	if p == nil {
		http.Error(w, fmt.Sprintf("no such producer: %s", req.Producer), http.StatusNotFound)
		return
	}
	trxId, err := p.BeginTrx()
	if err != nil {
		logger.Error("BeginTrx", zap.String("producer", req.Producer), zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("BeginTrx", zap.String("producer", req.Producer), zap.String("trxId", trxId))
	}

	writeJSONResponse(w, &BeginTrxResponse{
		TrxId: trxId,
	})
}

func (s *Server) handlePostRmqTrxCommit(w http.ResponseWriter, r *http.Request) {
	req, err := readJSONRequest[CommitTrxRequest](r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("CommitTrx", zap.String("producer", req.Producer), zap.String("trxId", req.TrxId))
	}

	p := s.producers[req.Producer]
	if p == nil {
		http.Error(w, fmt.Sprintf("no such producer: %s", req.Producer), http.StatusNotFound)
		return
	}
	err = p.CommitTrx(req.TrxId)
	if err != nil {
		logger.Error("CommitTrx", zap.String("producer", req.Producer), zap.String("trxId", req.TrxId), zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, &CommitTrxResponse{})
}

func (s *Server) handlePostRmqTrxRollback(w http.ResponseWriter, r *http.Request) {
	req, err := readJSONRequest[RollbackTrxRequest](r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("RollBackTrx", zap.String("producer", req.Producer), zap.String("trxId", req.TrxId))
	}

	p := s.producers[req.Producer]
	if p == nil {
		http.Error(w, fmt.Sprintf("no such producer: %s", req.Producer), http.StatusNotFound)
		return
	}
	err = p.RollBackTrx(req.TrxId)
	if err != nil {
		logger.Error("RollBackTrx", zap.String("producer", req.Producer), zap.String("trxId", req.TrxId), zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, &RollbackTrxResponse{})
}

func readJSONRequest[T any](r *http.Request) (*T, error) {
	defer r.Body.Close()
	var v T
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func writeJSONResponse(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(v)
}

func runServer() error {
	srv := &Server{
		cwg:       &sync.WaitGroup{},
		producers: make(map[string]*Producer),
	}
	err := srv.start()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /rmq/message", srv.handlePostRmqMessage)
	mux.HandleFunc("POST /rmq/trx/begin", srv.handlePostRmqTrxBegin)
	mux.HandleFunc("POST /rmq/trx/commit", srv.handlePostRmqTrxCommit)
	mux.HandleFunc("POST /rmq/trx/rollback", srv.handlePostRmqTrxRollback)

	hsrv := &http.Server{
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			return app.ctx
		},
	}

	logger.Info("listening", zap.String("addr", app.config.Http.Listen))

	ln, err := net.Listen("tcp", app.config.Http.Listen)
	if err != nil {
		srv.stop()
		return err
	}

	addOnExit(func() {
		logger.Info("shutting down server")

		err := hsrv.Shutdown(context.Background())
		if err != nil {
			logger.Error("http shutdown", zap.Error(err))
		}

		srv.stop()
	})
	err = hsrv.Serve(ln)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
