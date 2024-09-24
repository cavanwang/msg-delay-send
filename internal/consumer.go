package internal

import (
	"context"
	"sync"
	"time"

	"github.com/cavanwang/msg-delay-send/pkg"
	k "github.com/segmentio/kafka-go"
	log "github.com/beego/beego/v2/core/logs"
)

type Consumer struct {
	workerCount int
	kafkaCfg k.ReaderConfig
	deliveryFn DeliveryFunc
	deliveryURL string
}

type consumerWorker struct {
	r *k.Reader
	deliveryFunc DeliveryFunc
	deliverURL string
}

type DeliveryFunc func(msg string, url string) error

func NewConsumer(workerCount int, kafkaCfg k.ReaderConfig, deliveryFn DeliveryFunc, deliveryURL string) *Consumer{
	return &Consumer{workerCount: workerCount, kafkaCfg: kafkaCfg, deliveryFn: deliveryFn, deliveryURL: deliveryURL}
}

func NewConsumerWorker(kafkaCfg k.ReaderConfig, deliveryFn DeliveryFunc, deliverURL string) *consumerWorker {
	r := k.NewReader(kafkaCfg)
	return &consumerWorker{r: r, deliveryFunc: deliveryFn, deliverURL: deliverURL}
}

func (c *Consumer) Consume(ctx context.Context, ) {
	wg := sync.WaitGroup{}
	for i := 0; i < c.workerCount; i++ {
		worker := NewConsumerWorker(c.kafkaCfg, c.deliveryFn, c.deliveryURL)
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Consume(ctx)
		}()
	}
	wg.Wait()
}

func (c *consumerWorker) Consume(ctx context.Context) {
	defer c.r.Close()

	for {
		select {
		case <-ctx.Done():
			log.Info("consumer stopped")
			return
		default:
		}

		// 从 Kafka 读取消息
		msg, err := c.r.FetchMessage(ctx)
		if err != nil {
			log.Error("read message error: %v", err)
			continue
		}
		kmsg := KafkaMsg{}
		pkg.FromJsonByte(msg.Value, &kmsg)

		// DB中插入消息记录，标记消息为投递中
		dbMsg := Message{CampaignID: kmsg.CampaignID, PhoneNumber: kmsg.PhoneNumber, Status: MsgStatusDilivering, UpdateTime: time.Now()}
		_, err = InsertMsg(dbMsg)
		if err != nil { // 插入报错，查看是否为重复插入导致
			if !pkg.IsDBDuplicateError(err) {
				log.Error("failed to insert message %+v: %v", dbMsg, err)
				continue
			}
			// 重复插入了，说明正在其他消费者处理中或者已经处理完毕
			m, err := GetMsg(dbMsg.CampaignID, dbMsg.PhoneNumber)
			if err != nil {
				log.Error("try go get msg %+v failed: %v", dbMsg, err)
				continue
			}
			if m.Status == MsgStatusDilivered { // 已经投递，忽略本次操作
				log.Debug("msg %+v already delivered, ignore the action", dbMsg)
				goto commitKafka
			}
			// 状态为投递中(尚未投递),需要再次投递
			dbMsg = m
		}

		// 调用发送函数投递该消息
		if err := c.deliveryFunc(kmsg.Msg, c.deliverURL); err != nil {
			log.Error("deliver msg %s to %s error: %v", kmsg.Msg, c.deliverURL, err)
			continue
		}

		// 更新DB消息记录为已投递
		dbMsg.UpdateTime = time.Now()
		dbMsg.Status = MsgStatusDilivered
		if err := UpdateMsg(dbMsg); err != nil {
			log.Error("UpdateMsg with %+v error: %v", dbMsg, err)
			continue
		}

		// 向kafka完成消息提交
		commitKafka: 
		if err := c.r.CommitMessages(ctx, msg); err != nil {
			log.Error("CommitMessages with %+v error: %v", msg, err)
			continue
		}
		log.Info("message %+v delivered successfully", dbMsg)
	}
}
