package main

import "time"

type ConsumerMessage struct {
	MessageId         string            `json:"messageId"`
	Topic             string            `json:"topic"`
	ConsumerGroup     string            `json:"consumerGroup"`
	Body              []byte            `json:"body"`
	Properties        map[string]string `json:"properties"`
	Tag               string            `json:"tag"`
	Keys              []string          `json:"keys"`
	DeliveryTimestamp int64             `json:"deliveryTimestamp"`
}

type SendMessageRequest struct {
	Producer     string     `json:"producer"`
	TrxId        string     `json:"trxId,omitempty"`
	Topic        string     `json:"topic"`
	Body         []byte     `json:"body"`
	Tag          string     `json:"tag,omitempty"`
	MessageGroup string     `json:"messageGroup,omitempty"`
	Keys         []string   `json:"keys,omitempty"`
	DelayTime    *time.Time `json:"delayTime,omitempty"`
}

type SendMessageResponse struct {
	Receipts []*SendReceipt `json:"receipts"`
}

type SendReceipt struct {
	MessageId     string `json:"messageId"`
	TransactionId string `json:"transactionId"`
	Offset        int64  `json:"offset"`
}

type BeginTrxRequest struct {
	Producer string `json:"producer"`
}

type BeginTrxResponse struct {
	TrxId string `json:"trxId"`
}

type CommitTrxRequest struct {
	Producer string `json:"producer"`
	TrxId    string `json:"trxId"`
}

type CommitTrxResponse struct {
}

type RollbackTrxRequest struct {
	Producer string `json:"producer"`
	TrxId    string `json:"trxId"`
}

type RollbackTrxResponse struct {
}
