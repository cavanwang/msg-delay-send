package internal

import (
	"context"
	"encoding/csv"
	"html/template"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cavanwang/msg-delay-send/pkg"
	k "github.com/segmentio/kafka-go"
	log "github.com/beego/beego/v2/core/logs"
)

const (
	sendChannelLen = 1000
	// 每次从DB取100条到期的活动
	batchCampaignCount = 100
	// 如果当前没有待发送的活动则睡眠一段时间
	sleepSecondsWhenNoDelivery = 1
	// 发送消息超时时间
	sendMsgTimeout = 2 * time.Second
)

type Producer struct {
	kafkaCfg k.WriterConfig
	// 并发发送kafka的协程数量
	sendKafkaWorkerCount int
}

type CSVRecord struct {
	// first column
	Username string
	// second column
	PhoneNumber string
}

type KafkaMsg struct {
	CampaignID  int64
	Username    string
	PhoneNumber string
	Msg         string
}

func NewProducer(kafkaCfg k.WriterConfig, workerCount int) *Producer {
	return &Producer{
		kafkaCfg:             kafkaCfg,
		sendKafkaWorkerCount: workerCount,
	}
}

func (p *Producer) Produce(ctx context.Context) {
	log.Info("produce started with config=%+v", p.kafkaCfg)
	for {
		select {
		case <-ctx.Done():
			log.Info("producer stopped")
			return
		default:
		}

		if campaignCount := p.do(ctx); campaignCount == 0 {
			log.Info("no campaigns to deliver, sleep for a while")
			time.Sleep(sleepSecondsWhenNoDelivery * time.Second)
		}
	}
}

// 每次从DB读取N个活动，然后并发向kafka发送者N个活动涉及的消息，全部发送完后在统一返回
func (p *Producer) do(ctx context.Context) (scanCampaignCount int) {
	childCtx, cancel := context.WithDeadline(ctx, time.Now().Add(sendMsgTimeout))
	defer cancel()

	// 读取DB中的待发送的活动
	campaigns, err := ListCampigns(time.Now(), batchCampaignCount)
	if err != nil {
		log.Error("ListCampigns error: %v", err)
		return
	}
	scanCampaignCount = len(campaigns)

	// 并发处理不同的活动发送
	wg := sync.WaitGroup{}
	for _, c := range campaigns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.handleOneCampaign(childCtx, c)
		}()
	}
	wg.Wait()
	return scanCampaignCount
}

func (p *Producer) handleOneCampaign(ctx context.Context, c Campaign) {
	log.Info("start handleOneCampaign with id=%v", c.ID)
	// 读取csv文件中的接收人信息
	csvRecords, err := p.readCSV(c.CSVPath)
	if err != nil {
		log.Error("readCSV error from %s: %v", c.CSVPath, err)
		return
	}

	// 渲染模板
	var msgs []KafkaMsg
	tmpl, err := template.New("msg.html").Parse(c.Template)
	if err != nil {
		log.Error("parse template campaign id=%v tpl=%s error=%v", c.ID, c.Template, err)
		return
	}
	var buf strings.Builder
	for _, r := range csvRecords {
		if err = tmpl.Execute(&buf, r); err != nil {
			log.Error("parse template from %s with %+v", c.Template, r)
			return
		}
		msgs = append(msgs, KafkaMsg{CampaignID: c.ID, Username: r.Username, PhoneNumber: r.PhoneNumber, Msg: buf.String()})
	}
	log.Debug("campaign=%v finished to read csv with %d records", c.ID, len(msgs))

	// 并发发送kafka生产者消息
	var kmsgs []k.Message
	for _, msg := range msgs {
		kmsgs = append(kmsgs, k.Message{Value: pkg.ToJsonBytes(msg)})
	}
	if ok := p.produceOneCampaign(ctx, c, kmsgs); !ok {
		log.Error("produceOneCampaign for %v failed", c.ID)
		return
	}
	log.Info("campaign %d's msgs are all sent to kafka, we will update campaign's db status", c.ID)

	// 更新活动状态为已处理
	if err := UpdateCampaignAllEnqueue(c.ID, true); err != nil {
		log.Error("UpdateCampaignAllEnqueue to %+v error: %v", c, err)
		return
	}
	log.Info("handleOneCampaign ok with id=%v", c.ID)
}

func (p *Producer) produceOneCampaign(ctx context.Context, c Campaign, msgs []k.Message) (ok bool) {
	// 并发协程批量发送消息给kafka
	wg := sync.WaitGroup{}
	var errHappend atomic.Bool
	ch := make(chan k.Message, sendChannelLen)

	// 启动workers并发发送kafka消息
	for i := 0; i < p.sendKafkaWorkerCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sender := k.NewWriter(p.kafkaCfg)
			defer sender.Close()
			for msg := range ch {
				err := sender.WriteMessages(ctx, msg)
				if err != nil {
					log.Error("send campaign %d's msg to kafka error: %v", c.ID, err)
					errHappend.Store(true)
					return
				}
				log.Debug("campaign %d's %dth goroutine successfully send a msg to kafka", c.ID, i+1)
			}
		}(i)
	}

	// 向通道发送全部消息
	for _, msg := range msgs {
		ch <- msg
	}
	close(ch)
	log.Info("all campaign %d's %d msgs are pushed to channel", c.ID, len(msgs))

	//等待全部消息发送完毕
	wg.Wait()

	return !errHappend.Load()
}

func (p *Producer) readCSV(fname string) ([]CSVRecord, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var records []CSVRecord
	i := 0
	for {
		i++
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		log.Debug("csv read file=%s lineNo=%d field_num=%d line=%#v", fname, i, len(line), line)
		records = append(records, CSVRecord{
			Username:    line[0],
			PhoneNumber: line[1],
		})
	}
	return records, nil
}

func (p *Producer) parseTemplate(templateInput string, csvRecord CSVRecord) (string, error) {
	tmpl, err := template.New("msg.html").Parse(templateInput)
	if err != nil {
		return "", err
	}

	var buf strings.Builder
	err = tmpl.Execute(&buf, csvRecord)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
