package pkg

import (
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	log "github.com/beego/beego/v2/core/logs"
)

var (
	ErrSenderBusy = errors.New("sender busy")
)

type KafkaProducer struct {
	brokers   []string
	topic     string
	producer  sarama.SyncProducer
	batchSize int
	toSend    chan toSendMsg
	config    *sarama.Config
	busy      chan bool
}

type KafkaMsg = []byte
type toSendMsg struct {
	Msgs            []KafkaMsg
	SentCountNotify chan int
}

func NewKafkaProducer(brokers []string, topic string, config *sarama.Config, batchSize int) (p *KafkaProducer, err error) {
	if config == nil {
		config = sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要 leader 和 follower 都确认
		config.Producer.Partitioner = sarama.NewRandomPartitioner // 随机选择一个 partition
		config.Producer.Return.Successes = true                   // 成功交付的消息将在 success channel 返回
	}

	// 创建同步生产者
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	log.Info("kafka NewSyncProducer created")

	return &KafkaProducer{
		brokers:   brokers,
		topic:     topic,
		producer:  producer,
		batchSize: batchSize,
		config:    config,
		busy:      make(chan bool, 1),
	}, nil
}

func (k *KafkaProducer) SendMessages(msgs []KafkaMsg) error {
	// 如果上次重连失败继续尝试重连
	if err := k.reconnect(false); err != nil {
		return err
	}

	// 发送消息
	var kmsgs []*sarama.ProducerMessage
	for _, msg := range msgs {
		kmsgs = append(kmsgs, &sarama.ProducerMessage{Topic: k.topic, Value: sarama.StringEncoder(msg)})
	}
	log.Debug("will send kafka %d messages", len(msgs))
	err := k.producer.SendMessages(kmsgs)
	if err != nil {
		log.Error("failed to send messages error=%v msgs=%s", err, ToJsonBytes(msgs))
		// 失败强制重连
		_err := k.reconnect(true)
		return fmt.Errorf("send message error=%w, reconnect error=%v", err, _err)
	}
	return nil
}

func (k *KafkaProducer) GrabSender() bool {
	select {
	case k.busy <- true:
		return true
	default:
		return false
	}
}

func (k *KafkaProducer) ReleaseSender() {
	<-k.busy
}

func (k *KafkaProducer) reconnect(force bool) error {
	startTime := time.Now()
	if force {
		if k.producer != nil {
			if err := k.producer.Close(); err != nil {
				log.Error("close producer error:", err)
				k.producer = nil
				return err
			}
		}
		k.producer = nil
	} else if k.producer != nil {
		return nil
	}

	producer, err := sarama.NewSyncProducer(k.brokers, k.config)
	if err != nil {
		log.Error("NewSyncProducer error: %v", err)
		k.producer = nil
		return err
	}
	k.producer = producer
	log.Info("reconnect to kafka ok with cost=%dms", time.Since(startTime)/time.Millisecond)
	return nil
}
