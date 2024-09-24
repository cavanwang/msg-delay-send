package internal

import (
	"context"
	"encoding/csv"
	"errors"
	"html/template"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	log "github.com/beego/beego/v2/core/logs"
	"github.com/cavanwang/msg-delay-send/pkg"
)

const (
	// 每次从DB取N条到期的活动
	batchCampaignCount = 5
	// 如果当前没有待发送的活动则睡眠一段时间
	sleepSecondsWhenNoDelivery = 1
)

type Producer struct {
	config  ProducerConfig
	senders []*pkg.KafkaProducer
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
	// 发送者设置的纳秒级时间戳，与message表中的RandID对比，如果不同表示当前消息是生产者重发的
	RandID int64
	Msg    string
}

type ProducerConfig struct {
	KafkaReadWriteTimeout time.Duration
	KafkaConnTimeout      time.Duration
	KafkaBatchSize        int
	Topic                 string
	Brokers               []string
	// 并发发送kafka的协程数量
	SendKafkaWorkerCount int
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	p := &Producer{
		config: config,
	}
	err := p.initKafkaSenders()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Producer) Produce(ctx context.Context) {
	log.Info("produce started with config=%+v", p.config)

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
			p.handleOneCampaign(ctx, c)
		}()
	}
	wg.Wait()
	return scanCampaignCount
}

func (p *Producer) handleOneCampaign(ctx context.Context, c Campaign) {
	startTime := time.Now()
	log.Info("start handleOneCampaign with id=%v", c.ID)
	// 读取csv文件中的接收人信息
	csvRecords, err := p.readCSV(c.CSVPath)
	if err != nil {
		log.Error("readCSV error from %s: %v", c.CSVPath, err)
		return
	}

	// 读取DB中消息记录，查看哪些尚未投递，只对尚未投递的进行kafka发送
	phones, err := ListMsgPhoneNums(c.ID)
	if err != nil {
		log.Error("ListMsgPhoneNums campaign=%d error=%v", c.ID, err)
		return
	}

	// 渲染模板
	var msgs []KafkaMsg
	tmpl, err := template.New("msg.html").Parse(c.Template)
	if err != nil {
		log.Error("parse template campaign id=%v tpl=%s error=%v", c.ID, c.Template, err)
		return
	}

	for _, r := range csvRecords {
		// 跳过DB已经被消费者标记过的消息
		if _, ok := phones[r.PhoneNumber]; ok {
			continue
		}
		var buf strings.Builder
		// 尚未投递的需要发送kafka
		if err = tmpl.Execute(&buf, r); err != nil {
			log.Error("parse template from %s with %+v", c.Template, r)
			return
		}
		msgs = append(msgs, KafkaMsg{CampaignID: c.ID, RandID: time.Now().UnixNano(), Username: r.Username, PhoneNumber: r.PhoneNumber, Msg: buf.String()})
	}
	log.Debug("campaign=%v finished to read csv with %d records", c.ID, len(msgs))

	// 并发发送kafka生产者消息
	var kmsgs []pkg.KafkaMsg
	for _, msg := range msgs {
		kmsgs = append(kmsgs, pkg.ToJsonBytes(msg))
	}
	if ok := p.produceOneCampaign(ctx, kmsgs); !ok {
		log.Error("produceOneCampaign for %v failed", c.ID)
		return
	}
	log.Info("campaign %d's msgs are all sent to kafka, we will update campaign's db status", c.ID)

	// 更新活动状态为已处理
	if err := UpdateCampaignAllEnqueue(c.ID, true); err != nil {
		log.Error("UpdateCampaignAllEnqueue to %+v error: %v", c, err)
		return
	}
	log.Info("handleOneCampaign ok with id=%v cost=%dms", c.ID, time.Since(startTime)/time.Millisecond)
}

func (p *Producer) produceOneCampaign(ctx context.Context, msgs []pkg.KafkaMsg) (ok bool) {
	if len(msgs) == 0 {
		return true
	}
	startTime := time.Now()

	// 向所有workers发送全部消息
	var notOk atomic.Bool
	wg := sync.WaitGroup{}
	i := 0
	msgsIndexEnd := 0
	for ; i < len(msgs); i = msgsIndexEnd {
		// 寻找并抢占空闲发送者
		start := time.Now()
		index := p.findIdleSender(ctx)
		if index < 0 {
			notOk.Store(true)
			break
		}
		cost := time.Since(start)
		msgsIndexEnd = min(i+p.config.KafkaBatchSize, len(msgs))

		// 启动协程发送本消息
		wg.Add(1)
		go func(index int, msgs []pkg.KafkaMsg, cost time.Duration) {
			defer wg.Done()
			defer p.senders[index].ReleaseSender() // 发送完毕解除占用
			// 开始同步发送一批消息
			startTime := time.Now()
			if err := p.senders[index].SendMessages(msgs); err != nil {
				notOk.Store(true)
			}
			log.Info("sent %d messages cost=%dms findidleCost=%dms", len(msgs), time.Since(startTime)/time.Millisecond, cost/time.Millisecond)
		}(index, msgs[i:msgsIndexEnd], cost)
		select {
		case <-ctx.Done():
			break
		default:
		}
	}
	// 等待该活动的全部消息发送完毕
	wg.Wait()
	log.Info("sent %d messages cost=%dms", len(msgs), time.Since(startTime)/time.Millisecond)
	return !notOk.Load() && i == len(msgs)
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

func (p *Producer) initKafkaSenders() error {
	wg := sync.WaitGroup{}
	lk := sync.Mutex{}
	p.senders = make([]*pkg.KafkaProducer, p.config.SendKafkaWorkerCount)
	var errMsgs []string
	for i := 0; i < p.config.SendKafkaWorkerCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			config := sarama.NewConfig()
			config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要 leader 和 follower 都确认
			config.Producer.Partitioner = sarama.NewRandomPartitioner // 随机选择一个 partition
			config.Producer.Return.Successes = true                   // 成功交付的消息将在 success channel 返回
			config.Net.DialTimeout = p.config.KafkaConnTimeout
			config.Net.ReadTimeout = p.config.KafkaReadWriteTimeout
			config.Net.WriteTimeout = p.config.KafkaReadWriteTimeout
			worker, err := pkg.NewKafkaProducer(p.config.Brokers, p.config.Topic, config, p.config.KafkaBatchSize)

			lk.Lock()
			defer lk.Unlock()
			if err != nil {
				errMsgs = append(errMsgs, err.Error())
			} else {
				p.senders[i] = worker
			}
		}(i)
	}
	wg.Wait()
	if len(errMsgs) > 0 {
		return errors.New(strings.Join(errMsgs, ";"))
	}
	return nil
}

func (p *Producer) findIdleSender(ctx context.Context) (index int) {
	start := int(rand.Uint() % uint(len(p.senders)))

	index = -1
	for index == -1 {
		for i := 0; i < len(p.senders); i++ {
			index = (i + start) % len(p.senders)
			if p.senders[index].GrabSender() {
				break
			}
			index = -1
		}
		select {
		case <-ctx.Done():
			return -1
		default:
		}
		if index == -1 {
			time.Sleep(time.Millisecond)
		}
	}

	return index
}
