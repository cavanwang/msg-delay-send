package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/astaxie/beego/orm"
	log "github.com/beego/beego/v2/core/logs"

	"github.com/cavanwang/msg-delay-send/internal"
)

func main() {
	logLevel := flag.Int("log_level", log.LevelInfo, "logging level: 3(error) 6(info) 7(debug)")

	// 初始化mysql配置
	param := internal.MysqlConnParam{}
	host := flag.String("mysql_host", "locallhost", "mysql host")
	port := flag.Int("mysql_port", 3306, "mysql port")
	username := flag.String("mysql_user", "root", "mysql username")
	password := flag.String("mysql_password", "123456", "mysql password")
	dbName := flag.String("mysql_dbname", "mydb", "mysql database name")
	maxIdleConn := flag.Int("mysql_idle_conn", 50, "mysql max idle connection")
	maxOpenConn := flag.Int("mysql_open_conn", 100, "mysql max open connection")
	orm.Debug = true

	// 初始化生产者配置
	kafkaHosts := flag.String("kafka_hosts", "127.0.0.1:9092", "kafka's listen address for consumer/producer accessing")
	topic := flag.String("topic", "campaign", "kafka topic for procuder/consumer accessing")
	workerCount := flag.Int("worker_count", 5, "how many goroutines to send msgs to kafka concurrently")
	connTimeout := flag.Int("conn_timeout", 10*1000, "timeout for connecting to kafka")
	readWriteTimeout := flag.Int("read_write_timeout", 2*1000, "timeout for reading from or sending to kafka by millisecond")
	batchSize := flag.Int("batch_size", 50, "number of messages for sending to kafka each time")
	flag.Parse()

	// 设置日志信息
	log.EnableFuncCallDepth(true)
	log.EnableFullFilePath(true)
	log.SetLevel(*logLevel)

	param.Host = *host
	param.Port = *port
	param.Username = *username
	param.Password = *password
	param.DBName = *dbName
	param.MaxIdleConn = *maxIdleConn
	param.MaxOpenConn = *maxOpenConn
	// 连接mysql
	internal.MustInitMysqlConn(param)
	log.Info("mysql init ok")

	// 注册信号监听，优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// 构建生成者对象
	hosts := strings.Split(*kafkaHosts, ",")
	cfg := internal.ProducerConfig{
		KafkaReadWriteTimeout: time.Duration(*readWriteTimeout) * time.Millisecond,
		KafkaConnTimeout:      time.Duration(*connTimeout) * time.Millisecond,
		KafkaBatchSize:        *batchSize,
		Topic:                 *topic,
		Brokers:               hosts,
		SendKafkaWorkerCount:  *workerCount,
	}
	producer, err := internal.NewProducer(cfg)
	if err != nil {
		log.Error("new producer: ", err)
		return
	}
	// 启动生产者服务
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		producer.Produce(ctx)
		log.Info("will notify signal exit")
		select {
		case sigCh <- syscall.SIGTERM:
		default:
		}
		log.Info("already notified signal exit")
	}()
	log.Info("producer starting")
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sigCh
		log.Info("signal will exit")
		cancel()
		log.Info("signal exited")
	}()

	wg.Wait()
	log.Info("producer exited")
}
