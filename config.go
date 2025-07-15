package main

import (
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	Log  *LogConfig  `yaml:"log"`
	Http *HttpConfig `yaml:"http"`
	Rmq  *RmqConfig  `yaml:"rmq"`
}

type LogConfig struct {
	Mode    string `yaml:"mode"`
	Level   string `yaml:"level"`
	Path    string `yaml:"path"`
	MaxSize int64  `yaml:"maxSize"`
	MaxAge  int    `yaml:"maxAge"`
}

type HttpConfig struct {
	Listen string `yaml:"listen"`
}

type RmqConfig struct {
	Log       *RmqLogConfig        `yaml:"log"`
	Callbacks []*RmqCallbackConfig `yaml:"callbacks"`
	Endpoints []*RmqEndpointConfig `yaml:"endpoints"`
	Producers []*RmqProducerConfig `yaml:"producers"`
	Consumers []*RmqConsumerConfig `yaml:"consumers"`
}

func (c *RmqConfig) GetCallback(name string) *RmqCallbackConfig {
	for _, callback := range c.Callbacks {
		if callback.Name == name {
			return callback
		}
	}
	return nil
}

func (c *RmqConfig) GetEndpoint(name string) *RmqEndpointConfig {
	for _, endpoint := range c.Endpoints {
		if endpoint.Name == name {
			return endpoint
		}
	}
	return nil
}

type RmqLogConfig struct {
	Root     string `yaml:"root"`
	FileName string `yaml:"fileName"`
	Level    string `yaml:"level"`
}

type RmqCallbackConfig struct {
	Name    string        `yaml:"name"`
	Url     string        `yaml:"url"`
	Timeout time.Duration `yaml:"timeout"`
}

type RmqEndpointConfig struct {
	Name         string `yaml:"name"`
	Endpoint     string `yaml:"endpoint"`
	Namespace    string `yaml:"namespace"`
	AccessKey    string `yaml:"accessKey"`
	AccessSecret string `yaml:"accessSecret"`
}

type RmqProducerConfig struct {
	Name               string        `yaml:"name"`
	EndpointRef        string        `yaml:"endpointRef"`
	SendTimeout        time.Duration `yaml:"sendTimeout"`
	TransactionTimeout time.Duration `yaml:"transactionTimeout"`
}

type RmqConsumerConfig struct {
	Name            string                        `yaml:"name"`
	EndpointRef     string                        `yaml:"endpointRef"`
	ConsumeGroup    string                        `yaml:"consumeGroup"`
	ConsumeType     string                        `yaml:"consumerType"`
	PullOptions     *RmqConsumerPullOptionsConfig `yaml:"pullOptions"`
	AckMode         string                        `yaml:"ackMode"`
	AllowNoCallback bool                          `yaml:"allowNoCallback"`
	Filters         []*RmqConsumerFilterConfig    `yaml:"filters"`
	Callbacks       []*RmqConsumerCallbackConfig  `yaml:"callbacks"`
}

type RmqConsumerPullOptionsConfig struct {
	AwaitDuration     time.Duration `yaml:"awaitDuration"`
	MaxMessageNum     int           `yaml:"maxMessageNum"`
	InvisibleDuration time.Duration `yaml:"invisibleDuration"`
}

type RmqConsumerFilterConfig struct {
	Topic      string `yaml:"topic"`
	Type       string `yaml:"type"`
	Expression string `yaml:"expression"`
}

type RmqConsumerCallbackConfig struct {
	Ref         string   `yaml:"ref"`
	Topics      []string `yaml:"topics"`
	IgnoreError bool     `yaml:"ignoreError"`
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
