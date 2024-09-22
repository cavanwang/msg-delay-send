package internal

import (
	"strconv"
	"time"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/beego/beego/v2/core/logs"
)

const (
	MsgStatusDilivering MessageStatus = iota
	MsgStatusDilivered
)

var (
	globalOrm orm.Ormer
)

type MessageStatus int

type MysqlConnParam struct {
	Host        string
	Port        int
	Username    string
	Password    string
	DBName      string
	MaxIdleConn int
	MaxOpenConn int
}

type Campaign struct {
	ID           int64     `orm:"column(id);auto;pk"`
	ScheduleTime time.Time `orm:"column(schedule_time)"`
	CSVPath      string    `orm:"column(csv_path);size(256)"`
	Template     string    `orm:"column(template);size(4096)"`
	AllEnqueue   bool      `orm:"column(all_enqueue)"`
}

type Recipient struct {
	PhoneNumber string `orm:"column(phone_number);pk;size(24)"`
	Name        string `orm:"column(name);size(64)"`
}

type Message struct {
	ID          int           `orm:"column(id);auto;pk"`
	CampaignID  int64         `orm:"column(campaign_id)"`
	PhoneNumber string        `orm:"column(phone_number);size(24)"`
	Status      MessageStatus `orm:"column(status)"`
	UpdateTime  time.Time     `orm:"column(update_time)"`
}

func (u *Message) TableUnique() [][]string {
	return [][]string{
		{"CampaignID", "PhoneNumber"},
	}
}

func MustInitMysqlConn(param MysqlConnParam) {
	// 注册数据库驱动
	if err := orm.RegisterDriver("mysql", orm.DRMySQL); err != nil {
		panic(err)
	}
	log.Info("mysql param is: %+v", param)

	// 设置数据库连接信息
	dbLink := param.Username + ":" +
		param.Password + "@tcp(" +
		param.Host + ":" +
		strconv.Itoa(param.Port) + ")/" +
		param.DBName + "?charset=utf8"

	// 注册数据库
	if err := orm.RegisterDataBase("default", "mysql", dbLink); err != nil {
		panic(err)
	}

	// 设置连接数限制
	db, err := orm.GetDB("default")
	if err != nil {
		panic(err)
	}
	db.SetMaxIdleConns(param.MaxIdleConn)
	db.SetMaxOpenConns(param.MaxOpenConn)

	// 注册模型
	orm.RegisterModel(new(Campaign))
	orm.RegisterModel(new(Recipient))
	orm.RegisterModel(new(Message))

	if err := orm.RunSyncdb("default", false, true); err != nil {
		panic(err)
	}

	// 初始化全局orm对象
	globalOrm = orm.NewOrm()
}

// 查询调度时间<=scheduleTime且handled为false的全部活动
func ListCampigns(scheduleTime time.Time, limit int) (result []Campaign, err error) {
	_, err = globalOrm.QueryTable(&Campaign{}).
		Filter("ScheduleTime__lte", scheduleTime).
		Filter("AllEnqueue", false).Limit(limit).All(&result)
	return result, err
}

// 更新活动记录为已被生产者发送
func UpdateCampaignAllEnqueue(id int64, allEnqueue bool) error {
	_, err := globalOrm.QueryTable(&Campaign{}).Filter("ID", id).Update(orm.Params{"AllEnqueue": allEnqueue})
	return err
}

func InsertMsg(msg Message) (id int64, err error) {
	return globalOrm.Insert(&msg)
}

func GetMsg(campaignID int64, phoneNumber string) (msg Message, err error) {
	err = globalOrm.QueryTable(&Message{}).Filter("CampaignID", campaignID).
		Filter("PhoneNumber", phoneNumber).One(&msg)
	return
}

func UpdateMsg(msg Message) error {
	_, err := globalOrm.Update(&msg)
	return err
}

func InsertCampaign(c Campaign) (int64, error) {
	return globalOrm.Insert(&c)
}
