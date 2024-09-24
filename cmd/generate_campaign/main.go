package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/astaxie/beego/orm"
	log "github.com/sirupsen/logrus"

	"github.com/cavanwang/msg-delay-send/internal"
)

const (
	tpl = `{{.Username}} 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/{{.PhoneNumber}}
`
)

func main() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors:   true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
	})
	log.SetLevel(log.InfoLevel)

	// 初始化mysql配置
	param := internal.MysqlConnParam{}
	host := flag.String("mysql_host", "locallhost", "mysql host")
	port := flag.Int("mysql_port", 3306, "mysql port")
	username := flag.String("mysql_user", "root", "mysql username")
	password := flag.String("mysql_password", "123456", "mysql password")
	dbName := flag.String("mysql_dbname", "mydb", "mysql database name")
	maxIdleConn := flag.Int("mysql_idle_conn", 50, "mysql max idle connection")
	maxOpenConn := flag.Int("mysql_open_conn", 100, "mysql max open connection")

	// 生成活动数量
	campaignCount := flag.Int("campaign_count", 20, "number of campaigns")
	// 每个活动生成的推送用户数量
	recipientCount := flag.Int("recipient_count", 30, "number of recipient for each campaign")
	flag.Parse()

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

	// 清理旧数据
	n, err := orm.NewOrm().QueryTable(&internal.Campaign{}).Filter("ID__gte", 0).Delete()
	log.Infof("delete %d old records, error=%v", n, err)

	rand.Seed(time.Now().UnixNano())
	// 逐个活动依次生成
	for i := 0; i < *campaignCount; i++ {
		c := internal.Campaign{
			ScheduleTime: time.Now().Add(time.Duration(time.Duration(rand.Uint64()) % (60 * time.Second))),
			CSVPath:      "./" + strconv.Itoa(i) + ".csv",
			Template:     tpl,
			AllEnqueue:   false,
		}
		// 生成用户列表并写入csv文件
		end := i*1e5 + *recipientCount
		content := fmt.Sprintf("user%d,%011d", i*1e5, i*1e5)
		for j := i * 1e5 + 1; j < end; j++ {
			content += fmt.Sprintf("\nuser%d,%011d", j, j)
		}
		if err := os.WriteFile(c.CSVPath, []byte(content), 0666); err != nil {
			panic(err)
		}
		// 插入活动记录表
		if _, err := internal.InsertCampaign(c); err != nil {
			panic(fmt.Sprintf("i=%d insert campaign error=%v", i, err))
		}
		if i%10 == 0 {
			log.Infof("inserted campaign %d", i)
		}
	}

	log.Infof("all campaign inserted: %d records", *campaignCount)
}
