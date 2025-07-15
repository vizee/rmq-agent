package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	rmqclient "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"go.uber.org/zap"
)

type Trx struct {
	inner rmqclient.Transaction
	done  chan struct{}
}

type Producer struct {
	prd         rmqclient.Producer
	sendTimeout time.Duration
	trxTimeout  time.Duration
	trxLock     sync.Mutex
	trxs        map[string]*Trx
}

func (p *Producer) Stop() error {
	p.trxLock.Lock()
	defer p.trxLock.Unlock()
	for _, trx := range p.trxs {
		err := trx.inner.RollBack()
		if err != nil {
			logger.Warn("transaction rollback", zap.Error(err))
		}
	}
	return p.prd.GracefulStop()
}

func (p *Producer) getTrx(trxId string, remove bool) (rmqclient.Transaction, error) {
	p.trxLock.Lock()
	defer p.trxLock.Unlock()
	tx := p.trxs[trxId]
	if tx == nil {
		return nil, fmt.Errorf("transaction %s not found or timeout", trxId)
	}
	if remove {
		if tx.done != nil {
			close(tx.done)
		}
		delete(p.trxs, trxId)
	}
	return tx.inner, nil
}

func (p *Producer) addTrx(trxId string) *Trx {
	p.trxLock.Lock()
	defer p.trxLock.Unlock()
	if p.trxs[trxId] == nil {
		var done chan struct{}
		if p.trxTimeout > 0 {
			done = make(chan struct{})
		}
		trx := &Trx{
			inner: p.prd.BeginTransaction(),
			done:  done,
		}
		p.trxs[trxId] = trx
		return trx
	}
	return nil
}

func (p *Producer) BeginTrx() (string, error) {
	for {
		var b [18]byte
		_, err := rand.Read(b[:])
		if err != nil {
			return "", err
		}
		trxId := base64.StdEncoding.EncodeToString(b[:])
		trx := p.addTrx(trxId)
		if trx != nil {
			if p.trxTimeout > 0 {
				go func() {
					timeout := time.NewTimer(p.trxTimeout)
					defer timeout.Stop()
					select {
					case <-timeout.C:
						tx, _ := p.getTrx(trxId, true)
						if tx != nil {
							err := tx.RollBack()
							if err != nil {
								logger.Warn("rollback timeout tx", zap.Error(err))
							}
						}
					case <-trx.done:
					}
				}()
			}
			return trxId, nil
		}
	}
}

func (p *Producer) CommitTrx(trxId string) error {
	tx, err := p.getTrx(trxId, true)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (p *Producer) RollBackTrx(trxId string) error {
	tx, err := p.getTrx(trxId, true)
	if err != nil {
		return err
	}
	return tx.RollBack()
}

func (p *Producer) SendMessage(req *SendMessageRequest) (*SendMessageResponse, error) {
	var trx rmqclient.Transaction
	if req.TrxId != "" {
		tx, err := p.getTrx(req.TrxId, false)
		if err != nil {
			return nil, err
		}
		trx = tx
	}
	var (
		sr  []*rmqclient.SendReceipt
		err error
	)
	msg := convertSendMessageToRmq(req)
	ctx := app.ctx
	if p.sendTimeout > 0 {
		cc, cancel := context.WithTimeout(ctx, p.sendTimeout)
		defer cancel()
		ctx = cc
	}
	if trx != nil {
		sr, err = p.prd.SendWithTransaction(ctx, msg, trx)
	} else {
		sr, err = p.prd.Send(ctx, msg)
	}
	if err != nil {
		return nil, err
	}
	return &SendMessageResponse{
		Receipts: convertSendReceiptFromRmq(sr),
	}, nil
}

func newProducer(producerConfig *RmqProducerConfig, rmqConfig *RmqConfig) (*Producer, error) {
	endpointConfig := rmqConfig.GetEndpoint(producerConfig.EndpointRef)
	prd, err := rmqclient.NewProducer(&rmqclient.Config{
		Endpoint:  endpointConfig.Endpoint,
		NameSpace: endpointConfig.Namespace,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    endpointConfig.AccessKey,
			AccessSecret: endpointConfig.AccessSecret,
		},
	})
	if err != nil {
		return nil, err
	}
	err = prd.Start()
	if err != nil {
		return nil, err
	}
	return &Producer{
		prd:         prd,
		sendTimeout: producerConfig.SendTimeout,
		trxTimeout:  producerConfig.TransactionTimeout,
	}, nil
}

func convertSendMessageToRmq(req *SendMessageRequest) *rmqclient.Message {
	msg := &rmqclient.Message{
		Topic: req.Topic,
		Body:  req.Body,
	}
	if req.Tag != "" {
		msg.SetTag(req.Tag)
	}
	if req.MessageGroup != "" {
		msg.SetMessageGroup(req.MessageGroup)
	}
	if len(req.Keys) > 0 {
		msg.SetKeys(req.Keys...)
	}
	if req.DelayTime != nil {
		msg.SetDelayTimestamp(*req.DelayTime)
	}
	return msg
}

func convertSendReceiptFromRmq(sr []*rmqclient.SendReceipt) []*SendReceipt {
	rs := make([]*SendReceipt, 0, len(sr))
	for _, r := range sr {
		rs = append(rs, &SendReceipt{
			MessageId:     r.MessageID,
			TransactionId: r.TransactionId,
			Offset:        r.Offset,
		})
	}
	return rs
}
