package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	rmqclient "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	appUA = "rmq-agent/0.1.0"
)

const (
	ackModeAll        = iota // 所有 callback 返回 200 后 ack
	ackModeAtLeastOne        // 至少有一个 callback 返回 200 后 ack
	ackModeAlways            // 忽略 callback 返回，总是 ack
)

var (
	ErrNoCallback  = errors.New("no callback")
	ErrNotEnoughOk = errors.New("not enough ok")
)

type Consumer interface {
	Name() string
	Run(ctx context.Context, wg *sync.WaitGroup) error
	Stop() error
}

type CallbackItem struct {
	cb          *RmqCallbackConfig
	topics      map[string]bool
	ignoreError bool
}

func (c *CallbackItem) handleMessage(ctx context.Context, msg *ConsumerMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cb.Url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", appUA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if logger.Level().Enabled(zap.DebugLevel) {
		var respBody bytes.Buffer
		io.Copy(&respBody, io.LimitReader(resp.Body, 4096))
		logger.Debug("callback response", zap.String("messageId", msg.MessageId), zap.String("name", c.cb.Name), zap.Int("statusCode", resp.StatusCode), zap.Stringer("body", &respBody))
	} else {
		io.Copy(io.Discard, resp.Body)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}
	return nil
}

type CallbackGroup struct {
	callbacks       []*CallbackItem
	ackMode         int
	allowNoCallback bool
}

func (g *CallbackGroup) handleMessage(ctx context.Context, consumerGroup string, msg *rmqclient.MessageView) error {
	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("handle message",
			zap.String("consumerGroup", consumerGroup),
			zap.String("messageId", msg.GetMessageId()),
			zap.String("topic", msg.GetTopic()),
			zap.Stringp("messageGroup", msg.GetMessageGroup()),
			zap.Int("body", len(msg.GetBody())),
			zap.Any("properties", msg.GetProperties()),
			zap.Stringp("tag", msg.GetTag()),
			zap.Strings("keys", msg.GetKeys()),
			zap.Timep("deliveryTimestamp", msg.GetDeliveryTimestamp()),
			zap.Int("deliveryAttempt", int(msg.GetDeliveryAttempt())))
	}

	var deliveryTimestamp int64
	if msg.GetDeliveryTimestamp() != nil {
		deliveryTimestamp = msg.GetDeliveryTimestamp().UnixMilli()
	} else {
		deliveryTimestamp = 0
	}
	rmsg := &ConsumerMessage{
		MessageId:         msg.GetMessageId(),
		Topic:             msg.GetTopic(),
		ConsumerGroup:     consumerGroup,
		Body:              msg.GetBody(),
		Properties:        msg.GetProperties(),
		Tag:               *cmp.Or(msg.GetTag(), new(string)),
		Keys:              msg.GetKeys(),
		DeliveryTimestamp: deliveryTimestamp,
	}
	totalCount := 0
	okCount := 0
	for _, item := range g.callbacks {
		if len(item.topics) > 0 && !item.topics[rmsg.Topic] {
			continue
		}
		totalCount++
		err := item.handleMessage(ctx, rmsg)
		if err != nil {
			if item.ignoreError {
				logger.Info("ignore callback error", zap.String("name", item.cb.Name), zap.String("topic", msg.GetTopic()), zap.Error(err))
			} else {
				logger.Error("callback error", zap.String("name", item.cb.Name), zap.String("topic", msg.GetTopic()), zap.Error(err))
			}
			if g.ackMode == ackModeAll {
				break
			}
			continue
		}
		okCount++
	}

	if logger.Level().Enabled(zap.DebugLevel) {
		logger.Debug("callback finish", zap.String("messageId", rmsg.MessageId), zap.Int("total", totalCount), zap.Int("okCount", okCount), zap.Int("totalCount", len(g.callbacks)))
	}

	if totalCount == 0 && !g.allowNoCallback {
		return ErrNoCallback
	}

	switch g.ackMode {
	case ackModeAll:
		if okCount == len(g.callbacks) {
			return nil
		}
	case ackModeAtLeastOne:
		if okCount > 0 {
			return nil
		}
	case ackModeAlways:
		return nil
	}
	return ErrNotEnoughOk
}

type PullConsumer struct {
	down              atomic.Bool
	name              string
	sc                rmqclient.SimpleConsumer
	consumerGroup     string
	maxMessageNum     int
	invisibleDuration time.Duration
	callbackGroup     *CallbackGroup
}

func (c *PullConsumer) Name() string {
	return c.name
}

func (c *PullConsumer) Stop() error {
	if !c.down.CompareAndSwap(false, true) {
		return errors.New("already stopped")
	}
	return c.sc.GracefulStop()
}

func (c *PullConsumer) Run(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()
	err := c.sc.Start()
	if err != nil {
		return err
	}
	for !c.down.Load() {
		if logger.Level().Enabled(zapcore.DebugLevel) {
			logger.Debug("pulling message", zap.String("name", c.name))
		}

		msgs, err := c.sc.Receive(ctx, int32(c.maxMessageNum), c.invisibleDuration)
		if err != nil {
			if err == context.Canceled {
				break
			}
			logger.Error("pull message", zap.String("name", c.name), zap.Error(err))
			continue
		}

		if len(msgs) == 0 {
			continue
		}

		if logger.Level().Enabled(zapcore.DebugLevel) {
			logger.Debug("pull message", zap.String("name", c.name), zap.Int("num", len(msgs)))
		}

		for _, msg := range msgs {
			err := c.callbackGroup.handleMessage(ctx, c.consumerGroup, msg)
			if err != nil {
				logger.Error("handle message", zap.String("topic", msg.GetTopic()), zap.String("messageId", msg.GetMessageId()), zap.Error(err))
				continue
			}

			logger.Info("ack message", zap.String("messageId", msg.GetMessageId()))

			err = c.sc.Ack(ctx, msg)
			if err != nil {
				logger.Error("ack message", zap.String("messageId", msg.GetMessageId()), zap.Error(err))
			}
		}
	}

	logger.Info("consumer stopped", zap.String("name", c.name))
	return nil
}

func newPullConsumer(consumerConfig *RmqConsumerConfig, rmqConfig *RmqConfig) (Consumer, error) {
	endpointConfig := rmqConfig.GetEndpoint(consumerConfig.EndpointRef)
	if endpointConfig == nil {
		return nil, fmt.Errorf("endpoint not found: %s", consumerConfig.EndpointRef)
	}
	callbackGroup, err := getConsumerCallbackGroup(consumerConfig, rmqConfig)
	if err != nil {
		return nil, err
	}
	filterExpressions, err := getFilterExpressions(consumerConfig.Filters)
	if err != nil {
		return nil, err
	}
	var opts []rmqclient.SimpleConsumerOption
	if consumerConfig.PullOptions.AwaitDuration > 0 {
		opts = append(opts, rmqclient.WithAwaitDuration(consumerConfig.PullOptions.AwaitDuration))
	}
	opts = append(opts, rmqclient.WithSubscriptionExpressions(filterExpressions))
	c, err := rmqclient.NewSimpleConsumer(&rmqclient.Config{
		Endpoint:      endpointConfig.Endpoint,
		NameSpace:     endpointConfig.Namespace,
		ConsumerGroup: consumerConfig.ConsumeGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    endpointConfig.AccessKey,
			AccessSecret: endpointConfig.AccessSecret,
		},
	}, opts...)
	if err != nil {
		return nil, err
	}
	maxMessageNum := consumerConfig.PullOptions.MaxMessageNum
	if maxMessageNum <= 0 {
		maxMessageNum = 16
	}
	invisibleDuration := consumerConfig.PullOptions.InvisibleDuration
	if invisibleDuration <= 0 {
		invisibleDuration = time.Second * 30
	}
	return &PullConsumer{
		name:              consumerConfig.Name,
		sc:                c,
		consumerGroup:     consumerConfig.ConsumeGroup,
		maxMessageNum:     maxMessageNum,
		invisibleDuration: invisibleDuration,
		callbackGroup:     callbackGroup,
	}, nil
}

func getConsumerCallbackGroup(consumerConfig *RmqConsumerConfig, rmqConfig *RmqConfig) (*CallbackGroup, error) {
	if len(consumerConfig.Callbacks) == 0 {
		return nil, ErrNoCallback
	}

	var ackMode int
	switch consumerConfig.AckMode {
	case "all":
		ackMode = ackModeAll
	case "at-least-one":
		ackMode = ackModeAtLeastOne
	case "always":
		ackMode = ackModeAlways
	default:
		return nil, fmt.Errorf("unknown ack mode: %s", consumerConfig.AckMode)
	}

	var callbackItems []*CallbackItem
	for _, cb := range consumerConfig.Callbacks {
		calbackConfig := rmqConfig.GetCallback(cb.Ref)
		if calbackConfig == nil {
			return nil, fmt.Errorf("callback not found: %s", cb.Ref)
		}
		topics := make(map[string]bool, len(cb.Topics))
		for _, topic := range cb.Topics {
			topics[topic] = true
		}
		callbackItems = append(callbackItems, &CallbackItem{
			cb:          calbackConfig,
			topics:      topics,
			ignoreError: cb.IgnoreError,
		})
	}

	return &CallbackGroup{
		ackMode:         ackMode,
		callbacks:       callbackItems,
		allowNoCallback: consumerConfig.AllowNoCallback,
	}, nil
}

func getFilterExpressions(filters []*RmqConsumerFilterConfig) (map[string]*rmqclient.FilterExpression, error) {
	expressions := make(map[string]*rmqclient.FilterExpression, len(filters))
	for _, fc := range filters {
		switch fc.Type {
		case "tag":
			expressions[fc.Topic] = rmqclient.NewFilterExpressionWithType(fc.Expression, rmqclient.TAG)
		case "sql":
			expressions[fc.Topic] = rmqclient.NewFilterExpressionWithType(fc.Expression, rmqclient.SQL92)
		case "all":
			expressions[fc.Topic] = rmqclient.SUB_ALL
		default:
			return nil, fmt.Errorf("unknown filter type: %s", fc.Type)
		}
	}
	return expressions, nil
}
