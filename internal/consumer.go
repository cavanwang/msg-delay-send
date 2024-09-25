package internal

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	log "github.com/beego/beego/v2/core/logs"
	"github.com/cavanwang/msg-delay-send/pkg"
	k "github.com/segmentio/kafka-go"
)

const (
	maxBatchMsgCount      = 100
	maxBatchMsgTime       = 100 * time.Millisecond
	consumerSenderChanLen = 100
)

type Consumer struct {
	workerCount int
	kafkaCfg    k.ReaderConfig
	deliveryFn  DeliveryFunc
	deliveryURL string
}

type consumerReadWorker struct {
	r            *k.Reader
	deliveryFunc DeliveryFunc
	deliverURL   string
	sender       *pkg.KafkaProducer
	id           int64
}

type DeliveryFunc func(msg string, url string) error

func NewConsumer(workerCount int, kafkaCfg k.ReaderConfig, deliveryFn DeliveryFunc, deliveryURL string) *Consumer {
	return &Consumer{workerCount: workerCount, kafkaCfg: kafkaCfg, deliveryFn: deliveryFn, deliveryURL: deliveryURL}
}

func NewconsumerReadWorker(id int64, kafkaCfg k.ReaderConfig, deliveryFn DeliveryFunc, deliverURL string) (*consumerReadWorker, error) {
	r := k.NewReader(kafkaCfg)
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要 leader 和 follower 都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 随机选择一个 partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在 success channel 返回
	sender, err := pkg.NewKafkaProducer(id, kafkaCfg.Brokers, kafkaCfg.Topic, config, maxBatchMsgCount, consumerSenderChanLen)
	if err != nil {
		return nil, err
	}
	return &consumerReadWorker{r: r, deliveryFunc: deliveryFn, deliverURL: deliverURL, sender: sender, id: id}, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	wg := sync.WaitGroup{}
	for i := 0; i < c.workerCount; i++ {
		worker, err := NewconsumerReadWorker(int64(i), c.kafkaCfg, c.deliveryFn, c.deliveryURL)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Consume(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (c *consumerReadWorker) Consume(ctx context.Context) {
	defer c.r.Close()

	for {
		select {
		case <-ctx.Done():
			log.Info("%d: consumer stopped", c.id)
			return
		default:
		}

		// 批量从kafka读取n条消息
		msgs, err := c.batchFetchMessages(ctx)
		if err != nil {
			log.Error("%d: batchFetchMessages error: ", c.id, err)
			continue
		}
		if len(msgs) == 0 {
			continue
		}
		// 并发处理n条消息
		log.Info("%d: will handle %d msgs", c.id, len(msgs))
		failedMsgs := c.batchConsumeMsgs(ctx, msgs)
		if len(failedMsgs) == 0 {
			log.Info("batchConsumeMsgs %d msgs ok", len(msgs))
			continue
		}

		// 失败的消息需要重新投递回去
		var kmsgs []pkg.KafkaMsg
		for _, m := range failedMsgs {
			kmsgs = append(kmsgs, m.Value)
		}
		if err := c.sender.SendMessages(0, kmsgs); err != nil {
			log.Error("re-send %d messages error: %v", len(kmsgs), err)
			continue
		}
		log.Info("successfuly to re-send %d messages", len(kmsgs))
	}
}

func (c *consumerReadWorker) batchConsumeMsgs(ctx context.Context, msgs []k.Message) (failedMsgs []k.Message) {
	wg := sync.WaitGroup{}
	var errMsgs []string
	var lk sync.Mutex
	startTime := time.Now()
	for _, m := range msgs {
		wg.Add(1)
		go func(msg k.Message) {
			defer wg.Done()
			if err := c.handleOneMsg(msg); err != nil {
				lk.Lock()
				errMsgs = append(errMsgs, err.Error())
				failedMsgs = append(failedMsgs, msg)
				lk.Unlock()
			}
		}(m)
	}
	wg.Wait()
	// 批量提交消息， 如果失败进程需要挂掉，需要人工介入或者报警
	if err := c.r.CommitMessages(ctx, msgs...); err != nil {
		log.Emergency("batch commit %d msgs error: %v", len(msgs), err)
		log.GetBeeLogger().Flush()
		os.Exit(123)
	}
	if len(errMsgs) > 0 {
		log.Error("batch consume %d msgs error: %v", len(msgs), errMsgs)
		return failedMsgs
	}
	log.Info("%d: batch consume %d msgs ok, cost=%dms", c.id, len(msgs), time.Since(startTime)/time.Millisecond)
	return nil
}

func (c *consumerReadWorker) handleOneMsg(msg k.Message) error {
	kmsg := KafkaMsg{}
	pkg.FromJsonByte(msg.Value, &kmsg)

	// DB中插入消息记录，标记消息为投递中
	dbMsg := Message{CampaignID: kmsg.CampaignID, PhoneNumber: kmsg.PhoneNumber, Status: MsgStatusDilivering, RandID: kmsg.RandID, UpdateTime: time.Now()}
	id, err := InsertMsg(dbMsg)
	if err != nil { // 插入报错，查看是否为重复插入导致
		if !pkg.IsDBDuplicateError(err) {
			log.Error("failed to insert message %+v: %v", dbMsg, err)
			return err
		}
		// 重复插入了，说明正在其他消费者处理中或者已经处理完毕
		m, err := GetMsg(dbMsg.CampaignID, dbMsg.PhoneNumber)
		if err != nil {
			log.Error("try go get msg %+v failed: %v", dbMsg, err)
			return err
		}
		if m.Status == MsgStatusDilivered { // 已经投递，忽略本次操作
			log.Debug("msg %+v already delivered, ignore the action", dbMsg)
			return nil
		}
		if m.RandID != dbMsg.RandID { // 生产者重发的消息，忽略掉
			return nil
		}
		// 状态为投递中(尚未投递),需要再次投递
		dbMsg = m
	} else {
		dbMsg.ID = id
	}

	// 调用发送函数投递该消息
	if err := c.deliveryFunc(kmsg.Msg, c.deliverURL); err != nil {
		log.Error("deliver msg %s to %s error: %v", kmsg.Msg, c.deliverURL, err)
		return err
	}

	// 更新DB消息记录为已投递
	dbMsg.UpdateTime = time.Now()
	dbMsg.Status = MsgStatusDilivered
	if err := UpdateMsg(dbMsg); err != nil {
		log.Error("UpdateMsg with %+v error: %v", dbMsg, err)
		return err
	}
	return nil
}

func (c *consumerReadWorker) batchFetchMessages(ctx context.Context) (msgs []k.Message, err error) {
	startTime := time.Now()

	for i := 0; i < maxBatchMsgCount && time.Since(startTime) < maxBatchMsgTime; i++ {
		msg, err := c.r.FetchMessage(ctx)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
