package pkg

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	toSend    chan ToSendMsg
	config    *sarama.Config
	busy      chan bool
	id        int64
}

type KafkaMsg = []byte
type ToSendMsg struct {
	Msgs         []KafkaMsg
	SentOkNotify chan int
	CampaignID   int64

	SafeClose *SafeClose
}

type SafeClose struct {
	lock   sync.Mutex
	closed bool
}

func NewSafeClose() *SafeClose {
	return &SafeClose{lock: sync.Mutex{}, closed: false}
}

func (s *SafeClose) Close(ch chan int) {
	s.lock.Lock()
	if !s.closed {
		close(ch)
		s.closed = true
	}
	s.lock.Unlock()
}

func (s *SafeClose) Send(ch chan int, count int) {
	s.lock.Lock()
	if !s.closed {
		ch <- count
	}
	s.lock.Unlock()
}

func NewKafkaProducer(id int64, brokers []string, topic string, config *sarama.Config, batchSize int, toSendChanLen int) (p *KafkaProducer, err error) {
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
	log.Info("kafka NewSyncProducer %d created")

	return &KafkaProducer{
		brokers:   brokers,
		topic:     topic,
		producer:  producer,
		batchSize: batchSize,
		config:    config,
		busy:      make(chan bool, 1),
		toSend:    make(chan ToSendMsg, toSendChanLen),
		id:        id,
	}, nil
}

func (k *KafkaProducer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("kafka producer %d canceld", k.id)
			return
		case msgs := <-k.toSend:
			log.Debug("%d: will send %d batch messages from %d", k.id, len(msgs.Msgs), msgs.CampaignID)
			err := k.sendMessages(msgs.CampaignID, msgs.Msgs)
			n := 0
			if err != nil {
				log.Error("%d: send message from %d msgs=%s error=%v", k.id, msgs.CampaignID, msgs.Msgs[0], err)
			} else {
				n = len(msgs.Msgs)
				log.Debug("%d: sent %d batch messages ok from %d", k.id, len(msgs.Msgs), msgs.CampaignID)
			}
			msgs.SafeClose.Send(msgs.SentOkNotify, n)
		}
	}
}

func (k *KafkaProducer) PostBatchMessages(msgs ToSendMsg) {
	k.toSend <- msgs
}

func (k *KafkaProducer) sendMessages(campaignID int64, msgs []KafkaMsg) error {
	// 如果上次重连失败继续尝试重连
	if err := k.reconnect(false); err != nil {
		return err
	}

	// 发送消息
	var kmsgs []*sarama.ProducerMessage
	for _, msg := range msgs {
		kmsgs = append(kmsgs, &sarama.ProducerMessage{Topic: k.topic, Value: sarama.StringEncoder(msg)})
	}
	log.Debug("%d: will send kafka %d batch messages from %d", k.id, len(msgs), campaignID)
	err := k.producer.SendMessages(kmsgs)
	if err != nil {
		log.Error("%d: failed to send messages error=%v from %d msgs=%s", k.id, err, campaignID, ToJsonBytes(msgs))
		// 失败强制重连
		_err := k.reconnect(true)
		return fmt.Errorf("%d: send %d batch messages error=%w, reconnect error=%v from %d", k.id, len(msgs), err, _err, campaignID)
	}
	return nil
}

func (k *KafkaProducer) reconnect(force bool) error {
	startTime := time.Now()
	if force {
		if k.producer != nil {
			if err := k.producer.Close(); err != nil {
				log.Error("%d: close producer error:", k.id, err)
				k.producer = nil
				return err
			}
		}
		k.producer = nil
	} else if k.producer != nil {
		return nil
	}

	log.Debug("%d: will connect kafka", k.id)
	producer, err := sarama.NewSyncProducer(k.brokers, k.config)
	if err != nil {
		log.Error("%d: NewSyncProducer error: %v", k.id, err)
		k.producer = nil
		return err
	}
	k.producer = producer
	log.Info("%d: reconnect to kafka ok with cost=%dms", k.id, time.Since(startTime)/time.Millisecond)
	return nil
}
