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
	k "github.com/segmentio/kafka-go"

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

	// 初始化消费者配置
	kafkaHosts := flag.String("kafka_hosts", "127.0.0.1:9092", "kafka's listen address for consumer/producer accessing")
	topic := flag.String("topic", "campaign", "kafka topic for procuder/consumer accessing")
	workerCount := flag.Int("worker_count", 3, "how many goroutines to send msgs to kafka concurrently")
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
	kcfg := k.WriterConfig{
		Brokers:      hosts,
		Topic:        *topic,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	producer := internal.NewProducer(kcfg, *workerCount)
	// 启动生产者服务
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		producer.Produce(ctx)
		sigCh <- syscall.SIGTERM
	}()
	log.Info("producer starting")
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sigCh
		cancel()
	}()

	wg.Wait()
	log.Info("producer exited")
}
