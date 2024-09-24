# msg-delay-send
## 1、How to run(example on ubuntu22.04)
### 1.1 安装java
如果`java -version`显示缺少java命令，需要安装java：</br>
执行`apt-get update -y && apt install default-jdk -y`完成安装
### 1.2 安装kafka客户端
到https://kafka.apache.org/downloads下载合适的二进制压缩包后进行解压，比如：`wget https://downloads.apache.org/kafka/3.8.0/kafka_2.12-3.8.0.tgz && tar -xzvf kafka_2.12-3.8.0.tgz`
然后在~/.bashrc中配置相应环境变量，类似:</br>
<pre>export KAFKA_HOME=/root/kafka-deploy/kafka_2.13-3.8.0
export PATH=$KAFKA_HOME/bin:$PATH</pre></br>
### 1.3 在机器A上执行单节点zookeepr容器
`docker run -d --name kafka     --publish 9092:9092     --env KAFKA_BROKER_ID=0     --env KAFKA_ZOOKEEPER_CONNECT=<机器A的IP>:2181     --env KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092     --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<机器A的IP>:9092     wurstmeister/kafka`</br>
上述命令中需要替换<机器A的IP>为机器A的IP。
### 1.3 在机器A上执行单节点kafka集群启动命令
`docker run -d --name kafka     --publish 9092:9092     --env KAFKA_BROKER_ID=0     --env KAFKA_ZOOKEEPER_CONNECT=<机器A的IP>:2181     --env KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092     --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<机器A的IP>:9092     wurstmeister/kafka`</br>
上述命令中需要替换<机器A的IP>为机器A的IP。
### 1.4 在机器A上启动mysql数据库
执行:</br>
`mkdir mysql_data; cd mysql_data; docker run --name mysql -e MYSQL_ROOT_PASSWORD=123456 -p 3306:3306 -v $PWD/mysql_data:/var/lib/mysql -d mysql`</br>
启动mysql后，准备数据库mydb:</br>
执行如下命令连接到mysql:</br>
`docker exec -it mysql mysql -u root -p`</br>
然后输入密码123456后，执行创建数据库的命令:</br>
`create database mydb;`
### 1.5 编译csv文件活动数据的自动生产器
准备golang版本>=1.23.1。</br>
进入本代码库的cmd/generate_campaign目录下执行`go env -w GOOS=linux GOARCH=amd64; go build`, 然后将生成的二进制拷贝到机器A上，执行:</br>
`chmod +x generate_campaign; rm -rf *.csv; ./generate_campaign --campaign_count=10 --recipient_count=4  --mysql_host=127.0.0.1`</br>
这将生成有10个活动，每个活动对应的csv文件内包含4个活动接收者信息，类似如下执行过程:</br>
<pre>
root@node3:~/testme# chmod +x generate_campaign; rm -rf *.csv; ./generate_campaign --campaign_count=10 --recipient_count=4  --mysql_host=192.168.56.12
2024/09/24 19:39:58.059 [I]  mysql param is: {Host:192.168.56.12 Port:3306 Username:root Password:123456 DBName:mydb MaxIdleConn:50 MaxOpenConn:100}
table `campaign` already exists, skip
table `recipient` already exists, skip
table `message` already exists, skip
time="2024-09-24 19:39:58.155" level=info msg="mysql init ok"
time="2024-09-24 19:39:58.187" level=info msg="delete 10 old records, error=<nil>"
time="2024-09-24 19:39:58.233" level=info msg="inserted campaign 0"
time="2024-09-24 19:39:58.392" level=info msg="all campaign inserted: 10 records"
root@node3:~/testme# ls *.csv
0.csv  1.csv  2.csv  3.csv  4.csv  5.csv  6.csv  7.csv  8.csv  9.csv
root@node3:~/testme# cat 2.csv
user200000,00000200000
user200001,00000200001
user200002,00000200002
user200003,00000200003 
</pre>
### 1.6 初始化kafka主题
到机器A上执行主题相关命令:</br>
如果之前已经创建过campaign这个主题需要先删除(没创建过可以跳过)
`kafka-topics.sh --delete --topic campaign --bootstrap-server 127.0.0.1:9092`</br>
执行topic创建命令:</br>
`kafka-topics.sh --create --topic campaign --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2`</br>
类似如下:
<pre>
root@node1:~# kafka-topics.sh --delete --topic campaign --bootstrap-server 127.0.0.1:9092
root@node1:~# kafka-topics.sh --create --topic campaign --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2
Created topic campaign.
</pre>
### 1.7 生产者发送活动信息到kafka
进入代码库目录下的cmd/producer，执行编译命令:</br>
`go env -w GOOS=linux GOARCH=amd64; go build`, 然后将产出的二进制拷贝到机器A上。
在机器A上执行如下生产者命令:</br>
`chmod +x producer; ./producer --mysql_host=127.0.0.1 --log_level=6 --kafka_hosts=127.0.0.1:9092 --worker_count=3 --batch_size=3`
可以看到生产者生产消息的日志:
<pre>
root@node3:~/testme# chmod +x producer; ./producer --mysql_host=192.168.56.12 --log_level=6 --kafka_hosts=192.168.56.11:9092 --worker_count=3 --batch_size=3
2024/09/24 20:47:50.002 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/dboperation.go:65]  mysql param is: {Host:192.168.56.12 Port:3306 Username:root Password:123456 DBName:mydb MaxIdleConn:50 MaxOpenConn:100}
table `campaign` already exists, skip
table `recipient` already exists, skip
table `message` already exists, skip
2024/09/24 20:47:50.072 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:56]  mysql init ok
2024/09/24 20:47:50.112 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/pkg/kafkatopicsender.go:45]  kafka NewSyncProducer created
2024/09/24 20:47:50.112 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/pkg/kafkatopicsender.go:45]  kafka NewSyncProducer created
2024/09/24 20:47:50.116 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/pkg/kafkatopicsender.go:45]  kafka NewSyncProducer created
2024/09/24 20:47:50.117 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:91]  producer starting
2024/09/24 20:47:50.117 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:69]  produce started with config={KafkaReadWriteTimeout:2s KafkaConnTimeout:10s KafkaBatchSize:3 Topic:campaign Brokers:[192.168.56.11:9092] SendKafkaWorkerCount:3}
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    15.2ms] - [SELECT T0.`id`, T0.`schedule_time`, T0.`csv_path`, T0.`template`, T0.`all_enqueue` FROM `campaign` T0 WHERE T0.`schedule_time` <= ? AND T0.`all_enqueue` = ? LIMIT 5] - `2024-09-24 20:47:50`, `false`
2024/09/24 20:47:50.135 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1064
2024/09/24 20:47:50.138 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1060
2024/09/24 20:47:50.139 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1061
2024/09/24 20:47:50.139 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1062
2024/09/24 20:47:50.139 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1063
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    22.6ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1064`
2024/09/24 20:47:50.214 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=46ms findidleCost=0ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    94.2ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1063`
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    95.6ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1061`
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /   101.9ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1060`
2024/09/24 20:47:50.241 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=73ms findidleCost=0ms
2024/09/24 20:47:50.242 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=78ms
2024/09/24 20:47:50.242 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1064's msgs are all sent to kafka, we will update campaign's db status
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /   118.2ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1062`
2024/09/24 20:47:50.262 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=23ms findidleCost=0ms
2024/09/24 20:47:50.279 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=32ms findidleCost=4ms
2024/09/24 20:47:50.281 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=18ms findidleCost=16ms
2024/09/24 20:47:50.306 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=63ms
2024/09/24 20:47:50.306 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1060's msgs are all sent to kafka, we will update campaign's db status
2024/09/24 20:47:50.287 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=45ms findidleCost=0ms
2024/09/24 20:47:50.309 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=70ms
2024/09/24 20:47:50.309 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1063's msgs are all sent to kafka, we will update campaign's db status
2024/09/24 20:47:50.313 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=31ms findidleCost=39ms
2024/09/24 20:47:50.324 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=13ms findidleCost=0ms
2024/09/24 20:47:50.324 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=14ms findidleCost=47ms
2024/09/24 20:47:50.324 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=62ms
2024/09/24 20:47:50.324 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1062's msgs are all sent to kafka, we will update campaign's db status
2024/09/24 20:47:50.333 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=9ms findidleCost=42ms
2024/09/24 20:47:50.333 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=91ms
2024/09/24 20:47:50.333 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1061's msgs are all sent to kafka, we will update campaign's db status
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /   121.5ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1064`
2024/09/24 20:47:50.364 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1064 cost=229ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /    81.8ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1063`
2024/09/24 20:47:50.392 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1063 cost=252ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /    95.2ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1060`
2024/09/24 20:47:50.402 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1060 cost=264ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /   105.9ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1062`
2024/09/24 20:47:50.431 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1062 cost=292ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /    98.7ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1061`
2024/09/24 20:47:50.432 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1061 cost=293ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    12.6ms] - [SELECT T0.`id`, T0.`schedule_time`, T0.`csv_path`, T0.`template`, T0.`all_enqueue` FROM `campaign` T0 WHERE T0.`schedule_time` <= ? AND T0.`all_enqueue` = ? LIMIT 5] - `2024-09-24 20:47:50`, `false`
2024/09/24 20:47:50.448 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1069
2024/09/24 20:47:50.449 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1067
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /     8.7ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1069`
2024/09/24 20:47:50.449 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1068
2024/09/24 20:47:50.449 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1065
2024/09/24 20:47:50.449 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:111]  start handleOneCampaign with id=1066
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    17.3ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1065`
2024/09/24 20:47:50.493 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=20ms findidleCost=0ms
2024/09/24 20:47:50.495 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=22ms findidleCost=0ms
2024/09/24 20:47:50.495 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=22ms
2024/09/24 20:47:50.495 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1069's msgs are all sent to kafka, we will update campaign's db status
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    20.7ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1067`
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    39.1ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1068`
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    42.7ms] - [SELECT T0.`phone_number` FROM `message` T0 WHERE T0.`campaign_id` = ? ] - `1066`
2024/09/24 20:47:50.530 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=34ms findidleCost=1ms
2024/09/24 20:47:50.530 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=36ms findidleCost=0ms
2024/09/24 20:47:50.535 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=41ms
2024/09/24 20:47:50.538 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1065's msgs are all sent to kafka, we will update campaign's db status
2024/09/24 20:47:50.582 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=49ms findidleCost=0ms
2024/09/24 20:47:50.604 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=82ms findidleCost=0ms
2024/09/24 20:47:50.604 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=65ms findidleCost=5ms
2024/09/24 20:47:50.605 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=71ms
2024/09/24 20:47:50.605 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1066's msgs are all sent to kafka, we will update campaign's db status
2024/09/24 20:47:50.608 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=25ms findidleCost=61ms
2024/09/24 20:47:50.646 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=124ms
2024/09/24 20:47:50.646 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1068's msgs are all sent to kafka, we will update campaign's db status
2024/09/24 20:47:50.647 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 3 messages cost=41ms findidleCost=56ms
2024/09/24 20:47:50.640 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:200]  sent 1 messages cost=34ms findidleCost=0ms
2024/09/24 20:47:50.650 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:210]  sent 4 messages cost=101ms
2024/09/24 20:47:50.650 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:158]  campaign 1067's msgs are all sent to kafka, we will update campaign's db status
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /   183.4ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1069`
2024/09/24 20:47:50.679 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1069 cost=231ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /    93.0ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1066`
2024/09/24 20:47:50.698 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1066 cost=248ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /   160.0ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1065`
2024/09/24 20:47:50.698 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1065 cost=248ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /    67.6ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1067`
2024/09/24 20:47:50.718 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1067 cost=268ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /     db.Exec /    73.1ms] - [UPDATE `campaign` T0 SET T0.`all_enqueue` = ? WHERE T0.`id` = ? ] - `true`, `1068`
2024/09/24 20:47:50.722 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:165]  handleOneCampaign ok with id=1068 cost=272ms
[ORM]2024/09/24 20:47:50  -[Queries/default] - [  OK /    db.Query /    15.9ms] - [SELECT T0.`id`, T0.`schedule_time`, T0.`csv_path`, T0.`template`, T0.`all_enqueue` FROM `campaign` T0 WHERE T0.`schedule_time` <= ? AND T0.`all_enqueue` = ? LIMIT 5] - `2024-09-24 20:47:50`, `false`
2024/09/24 20:47:50.739 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:80]  no campaigns to deliver, sleep for a while
[ORM]2024/09/24 20:47:51  -[Queries/default] - [  OK /    db.Query /    11.1ms] - [SELECT T0.`id`, T0.`schedule_time`, T0.`csv_path`, T0.`template`, T0.`all_enqueue` FROM `campaign` T0 WHERE T0.`schedule_time` <= ? AND T0.`all_enqueue` = ? LIMIT 5] - `2024-09-24 20:47:51`, `false`
2024/09/24 20:47:51.753 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:80]  no campaigns to deliver, sleep for a while
[ORM]2024/09/24 20:47:52  -[Queries/default] - [  OK /    db.Query /    12.6ms] - [SELECT T0.`id`, T0.`schedule_time`, T0.`csv_path`, T0.`template`, T0.`all_enqueue` FROM `campaign` T0 WHERE T0.`schedule_time` <= ? AND T0.`all_enqueue` = ? LIMIT 5] - `2024-09-24 20:47:52`, `false`
2024/09/24 20:47:52.787 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:80]  no campaigns to deliver, sleep for a while
[ORM]2024/09/24 20:47:53  -[Queries/default] - [  OK /    db.Query /     3.1ms] - [SELECT T0.`id`, T0.`schedule_time`, T0.`csv_path`, T0.`template`, T0.`all_enqueue` FROM `campaign` T0 WHERE T0.`schedule_time` <= ? AND T0.`all_enqueue` = ? LIMIT 5] - `2024-09-24 20:47:53`, `false`
2024/09/24 20:47:53.816 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:80]  no campaigns to deliver, sleep for a while
^C2024/09/24 20:47:54.241 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:96]  signal will exit
2024/09/24 20:47:54.245 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:98]  signal exited
2024/09/24 20:47:54.818 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/producer.go:74]  producer stopped
2024/09/24 20:47:54.818 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:84]  will notify signal exit
2024/09/24 20:47:54.818 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:89]  already notified signal exit
2024/09/24 20:47:54.818 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/producer/main.go:102]  producer exited
root@node3:~/testme
</pre>
### 1.8 可以看到生产者标记数据库活动表中记录状态为已发送
参考如下命令查看数据库活动表信息:</br>
<pre>
root@node2:~# docker exec -it mysql mysql -u root -p
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 486
Server version: 9.0.1 MySQL Community Server - GPL

Copyright (c) 2000, 2024, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> use mydb;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+----------------+
| Tables_in_mydb |
+----------------+
| campaign       |
| message        |
| recipient      |
+----------------+
3 rows in set (0.00 sec)

mysql> desc campaign
    -> ;
+---------------+---------------+------+-----+---------+----------------+
| Field         | Type          | Null | Key | Default | Extra          |
+---------------+---------------+------+-----+---------+----------------+
| id            | bigint        | NO   | PRI | NULL    | auto_increment |
| schedule_time | datetime      | NO   |     | NULL    |                |
| csv_path      | varchar(256)  | NO   |     |         |                |
| template      | varchar(4096) | NO   |     |         |                |
| all_enqueue   | tinyint(1)    | NO   |     | 0       |                |
+---------------+---------------+------+-----+---------+----------------+
5 rows in set (0.47 sec)

mysql> select id, schedule_time, all_enqueue, csv_path from campaign;
+------+---------------------+-------------+----------+
| id   | schedule_time       | all_enqueue | csv_path |
+------+---------------------+-------------+----------+
| 1060 | 2024-09-24 11:40:49 |           1 | ./0.csv  |
| 1061 | 2024-09-24 11:39:10 |           1 | ./1.csv  |
| 1062 | 2024-09-24 11:40:04 |           1 | ./2.csv  |
| 1063 | 2024-09-24 11:40:38 |           1 | ./3.csv  |
| 1064 | 2024-09-24 11:40:19 |           1 | ./4.csv  |
| 1065 | 2024-09-24 11:40:53 |           1 | ./5.csv  |
| 1066 | 2024-09-24 11:39:43 |           1 | ./6.csv  |
| 1067 | 2024-09-24 11:39:42 |           1 | ./7.csv  |
| 1068 | 2024-09-24 11:39:18 |           1 | ./8.csv  |
| 1069 | 2024-09-24 11:40:06 |           1 | ./9.csv  |
+------+---------------------+-------------+----------+
10 rows in set (0.00 sec)

mysql> exit;
Bye
root@node2:~# 
</pre>
### 1.9 消费者消费消息
代码库cmd/consumer下执行`go env -w GOOS=linxu GOARCH=amd64; go build`编译消费者二进制，然后拷贝到机器A上执行:</br>
`chmod +x consumer; ./consumer --mysql_host=192.168.56.12 --log_level=6 --kafka_hosts=192.168.56.11:9092 --worker_count=3`</br>
可以看到类似如下消费者日志:
<pre>
root@node3:~/testme# chmod +x consumer; ./consumer --mysql_host=192.168.56.12 --log_level=6 --kafka_hosts=192.168.56.11:9092 --worker_count=3
2024/09/24 20:49:29.112 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/dboperation.go:65]  mysql param is: {Host:192.168.56.12 Port:3306 Username:root Password:123456 DBName:mydb MaxIdleConn:50 MaxOpenConn:100}
table `campaign` already exists, skip
table `recipient` already exists, skip
table `message` already exists, skip
2024/09/24 20:49:29.397 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:56]  mysql init ok
2024/09/24 20:49:29.397 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:87]  consumer started
2024/09/24 20:49:29.804 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user400000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000400000
 url=https://delivery-url/ delivered
2024/09/24 20:49:29.809 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user400003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000400003
 url=https://delivery-url/ delivered
2024/09/24 20:49:29.849 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1064 PhoneNumber:00000400000 Status:1 UpdateTime:2024-09-24 20:49:29.805283211 +0800 CST m=+0.709003625} delivered successfully
2024/09/24 20:49:29.871 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1064 PhoneNumber:00000400003 Status:1 UpdateTime:2024-09-24 20:49:29.811031162 +0800 CST m=+0.714751528} delivered successfully
2024/09/24 20:49:29.891 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user300003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000300003
 url=https://delivery-url/ delivered
2024/09/24 20:49:29.903 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user400001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000400001
 url=https://delivery-url/ delivered
2024/09/24 20:49:29.920 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1063 PhoneNumber:00000300003 Status:1 UpdateTime:2024-09-24 20:49:29.89196712 +0800 CST m=+0.795687482} delivered successfully
2024/09/24 20:49:29.936 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1064 PhoneNumber:00000400001 Status:1 UpdateTime:2024-09-24 20:49:29.904131859 +0800 CST m=+0.807852234} delivered successfully
2024/09/24 20:49:29.987 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user2 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000000002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.012 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1060 PhoneNumber:00000000002 Status:1 UpdateTime:2024-09-24 20:49:29.989886992 +0800 CST m=+0.893607383} delivered successfully
2024/09/24 20:49:30.036 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user400002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000400002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.065 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user3 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000000003
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.091 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1064 PhoneNumber:00000400002 Status:1 UpdateTime:2024-09-24 20:49:30.036948195 +0800 CST m=+0.940668564} delivered successfully
2024/09/24 20:49:30.114 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1060 PhoneNumber:00000000003 Status:1 UpdateTime:2024-09-24 20:49:30.093095905 +0800 CST m=+0.996816281} delivered successfully
2024/09/24 20:49:30.132 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user0 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000000000
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.157 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user300000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000300000
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.157 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1060 PhoneNumber:00000000000 Status:1 UpdateTime:2024-09-24 20:49:30.132699051 +0800 CST m=+1.036419424} delivered successfully
2024/09/24 20:49:30.174 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1063 PhoneNumber:00000300000 Status:1 UpdateTime:2024-09-24 20:49:30.157540266 +0800 CST m=+1.061260634} delivered successfully
2024/09/24 20:49:30.195 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user1 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000000001
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.207 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user300001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000300001
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.219 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1060 PhoneNumber:00000000001 Status:1 UpdateTime:2024-09-24 20:49:30.195769523 +0800 CST m=+1.099489878} delivered successfully
2024/09/24 20:49:30.229 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1063 PhoneNumber:00000300001 Status:1 UpdateTime:2024-09-24 20:49:30.207657932 +0800 CST m=+1.111378301} delivered successfully
2024/09/24 20:49:30.331 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user100000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000100000
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.350 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1061 PhoneNumber:00000100000 Status:1 UpdateTime:2024-09-24 20:49:30.331660382 +0800 CST m=+1.235380746} delivered successfully
2024/09/24 20:49:30.367 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user300002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000300002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.397 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1063 PhoneNumber:00000300002 Status:1 UpdateTime:2024-09-24 20:49:30.368021708 +0800 CST m=+1.271742098} delivered successfully
2024/09/24 20:49:30.398 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user200001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000200001
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.418 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1062 PhoneNumber:00000200001 Status:1 UpdateTime:2024-09-24 20:49:30.398991975 +0800 CST m=+1.302712342} delivered successfully
2024/09/24 20:49:30.432 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user100001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000100001
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.448 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1061 PhoneNumber:00000100001 Status:1 UpdateTime:2024-09-24 20:49:30.432893024 +0800 CST m=+1.336613378} delivered successfully
2024/09/24 20:49:30.457 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user200003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000200003
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.475 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1062 PhoneNumber:00000200003 Status:1 UpdateTime:2024-09-24 20:49:30.458153479 +0800 CST m=+1.361873858} delivered successfully
2024/09/24 20:49:30.484 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user100002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000100002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.515 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1061 PhoneNumber:00000100002 Status:1 UpdateTime:2024-09-24 20:49:30.485007045 +0800 CST m=+1.388727393} delivered successfully
2024/09/24 20:49:30.515 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user900003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000900003
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.539 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1069 PhoneNumber:00000900003 Status:1 UpdateTime:2024-09-24 20:49:30.516694214 +0800 CST m=+1.420414570} delivered successfully
2024/09/24 20:49:30.561 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user200000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000200000
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.576 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user900000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000900000
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.634 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1069 PhoneNumber:00000900000 Status:1 UpdateTime:2024-09-24 20:49:30.601206119 +0800 CST m=+1.504926505} delivered successfully
2024/09/24 20:49:30.636 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1062 PhoneNumber:00000200000 Status:1 UpdateTime:2024-09-24 20:49:30.562949125 +0800 CST m=+1.466669485} delivered successfully
2024/09/24 20:49:30.666 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user900001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000900001
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.669 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user200002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000200002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.695 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1062 PhoneNumber:00000200002 Status:1 UpdateTime:2024-09-24 20:49:30.669853166 +0800 CST m=+1.573573530} delivered successfully
2024/09/24 20:49:30.698 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1069 PhoneNumber:00000900001 Status:1 UpdateTime:2024-09-24 20:49:30.669263744 +0800 CST m=+1.572984111} delivered successfully
2024/09/24 20:49:30.731 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user500000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000500000
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.747 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1065 PhoneNumber:00000500000 Status:1 UpdateTime:2024-09-24 20:49:30.731561197 +0800 CST m=+1.635281541} delivered successfully
2024/09/24 20:49:30.751 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user100003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000100003
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.779 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user500001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000500001
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.791 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1061 PhoneNumber:00000100003 Status:1 UpdateTime:2024-09-24 20:49:30.757689975 +0800 CST m=+1.661410351} delivered successfully
2024/09/24 20:49:30.797 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1065 PhoneNumber:00000500001 Status:1 UpdateTime:2024-09-24 20:49:30.779766212 +0800 CST m=+1.683486582} delivered successfully
2024/09/24 20:49:30.835 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user500002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000500002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.853 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1065 PhoneNumber:00000500002 Status:1 UpdateTime:2024-09-24 20:49:30.835770844 +0800 CST m=+1.739491213} delivered successfully
2024/09/24 20:49:30.859 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user900002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000900002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.877 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1069 PhoneNumber:00000900002 Status:1 UpdateTime:2024-09-24 20:49:30.859915168 +0800 CST m=+1.763635525} delivered successfully
2024/09/24 20:49:30.894 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user800002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000800002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.912 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user500003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000500003
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.915 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1068 PhoneNumber:00000800002 Status:1 UpdateTime:2024-09-24 20:49:30.894864702 +0800 CST m=+1.798585084} delivered successfully
2024/09/24 20:49:30.931 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1065 PhoneNumber:00000500003 Status:1 UpdateTime:2024-09-24 20:49:30.913091812 +0800 CST m=+1.816812188} delivered successfully
2024/09/24 20:49:30.962 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user600002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000600002
 url=https://delivery-url/ delivered
2024/09/24 20:49:30.985 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user600000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000600000
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.001 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1066 PhoneNumber:00000600002 Status:1 UpdateTime:2024-09-24 20:49:30.967514673 +0800 CST m=+1.871235060} delivered successfully
2024/09/24 20:49:31.023 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1066 PhoneNumber:00000600000 Status:1 UpdateTime:2024-09-24 20:49:30.988346236 +0800 CST m=+1.892066604} delivered successfully
2024/09/24 20:49:31.033 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user600003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000600003
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.049 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1066 PhoneNumber:00000600003 Status:1 UpdateTime:2024-09-24 20:49:31.033754977 +0800 CST m=+1.937475356} delivered successfully
2024/09/24 20:49:31.061 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user800000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000800000
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.090 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1068 PhoneNumber:00000800000 Status:1 UpdateTime:2024-09-24 20:49:31.061803163 +0800 CST m=+1.965523522} delivered successfully
2024/09/24 20:49:31.093 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user800003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000800003
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.107 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1068 PhoneNumber:00000800003 Status:1 UpdateTime:2024-09-24 20:49:31.093736426 +0800 CST m=+1.997456784} delivered successfully
2024/09/24 20:49:31.119 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user600001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000600001
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.133 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1066 PhoneNumber:00000600001 Status:1 UpdateTime:2024-09-24 20:49:31.11984174 +0800 CST m=+2.023562102} delivered successfully
2024/09/24 20:49:31.145 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user700003 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000700003
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.159 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1067 PhoneNumber:00000700003 Status:1 UpdateTime:2024-09-24 20:49:31.145764182 +0800 CST m=+2.049484553} delivered successfully
2024/09/24 20:49:31.177 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user800001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000800001
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.197 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user700000 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000700000
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.203 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1068 PhoneNumber:00000800001 Status:1 UpdateTime:2024-09-24 20:49:31.17768081 +0800 CST m=+2.081401186} delivered successfully
2024/09/24 20:49:31.223 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1067 PhoneNumber:00000700000 Status:1 UpdateTime:2024-09-24 20:49:31.198248663 +0800 CST m=+2.101969052} delivered successfully
2024/09/24 20:49:31.251 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user700001 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000700001
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.272 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1067 PhoneNumber:00000700001 Status:1 UpdateTime:2024-09-24 20:49:31.258635474 +0800 CST m=+2.162355861} delivered successfully
2024/09/24 20:49:31.281 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:101]  fakeDeliveryFunc: msg=user700002 你好, 我们正在推广一个活动，希望你对其感兴趣，可以点击这个活动链接:
	https://a.b.c.d/user/00000700002
 url=https://delivery-url/ delivered
2024/09/24 20:49:31.303 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:112]  message {ID:0 CampaignID:1067 PhoneNumber:00000700002 Status:1 UpdateTime:2024-09-24 20:49:31.284699661 +0800 CST m=+2.188420034} delivered successfully


^C2024/09/24 20:49:36.058 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:94]  signal handler exited
2024/09/24 20:49:36.061 [E] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:64]  read message error: context canceled
2024/09/24 20:49:36.062 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:56]  consumer stopped
2024/09/24 20:49:36.059 [E] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:64]  read message error: context canceled
2024/09/24 20:49:36.062 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:56]  consumer stopped
2024/09/24 20:49:36.059 [E] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:64]  read message error: context canceled
2024/09/24 20:49:36.063 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/internal/consumer.go:56]  consumer stopped
2024/09/24 20:49:38.680 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:85]  consumer exited
2024/09/24 20:49:38.680 [I] [D:/go_src/src/github.com/cavanwang/msg-delay-send/cmd/consumer/main.go:97]  consumer exited
root@node3:~/testme# 
</pre>
## 2、This project's architecture design
### 2.1 生产者
<pre>
  1、启动时初始化若干的kafka生产者sender（基于github.com/IBM/sarama），初始化之后n个sender已经与kafka建立了同步的会话连接（意味着随后就可以发送消息了）。</br>
  2、启动一个生产者主循环(Produce函数内的for ...)，每次去DB中扫描一些campaign活动表中是否有已经到达调度时间(schedule_time <= time.Now())的活动, 没有就睡眠n秒继续重复。否则就准备启动n个协程，每个协程负责一个活动的发送。我们称这个协程为s协程</br>
  3、s协程会读取活动相关的csv文件，解析里面的用户名和电话，然后基于活动模板渲染出特定的kafka消息，比如生成100条待发送消息。然后查询message数据库表，过滤掉message中已经存在的消息记录(表示已经消费过，由消费者组件消费消息时插入DB的)。这样100条可能只剩下70条。然后对70条数据按照一批一批的顺序发送给kafka（使用kafka的batch send messages函数)， 每次批量发送时使用一个第1步中创建好的sender，在一个单独的goroutine中发送（会首先轮询一遍sender数组查看哪一个sender此时空闲没有在发送数据)。</br>
  4、s协程发送全部活动消息到kafka后，会更新campaign活动表的all_enqueue为true， 这样生产者后续扫描活动表就不会重复处理。</br>
  5、全部s协程都处理完消息发送后，会统一WaitGroup.Wait()，然后重复第2步的扫描到达调度时间的活动记录。</br>
  6、这里没有在发送完一条消息时写入message表一个记录，是想减少DB写操作。当然，极端情况如果一个活动的对应的n条消息有一条发送kafka失败，整个活动下一轮还会重新扫描到。但是这里也加了一个预防措施，就是读取消费端生成的message表，也能一定程度上避免n条消息全部重发。</br>
  </pre>
  ### 2.2 消费者:
  <pre>
  1、启动后启动多个goroutine，每个goroutine代表相同消费者组中的一个实例。</br>
  2、使用"github.com/segmentio/kafka-go"作为消费者客户端，提供消费kafka消息的SDK。</br>
  3、从sdk的channel不断接收消息。然后解析消息中的活动ID和电话，并向DB中insert该消息。</br>
  4、如果插入失败且原因是违法唯一性索引，说明该消息曾经被消费者处理过，本次可能是生产者重发消息导致重复处理报错，或者是消费者之前处理一半后进程退出遗留的消息被kafka重新推送，此时需要去DB中查询一下该消息的状态。</br>
  如果状态为已推送则忽略该消息。</br>
  如果状态为推送中，且查询DB后发现二者的rand_id字段不同(该字段是生产者每次发送消息时取得当前纳秒时间戳), 说明是生产者重发消息，忽略掉。</br>
  否则说明消息是之前消费者处理后尚未推送前消费者挂了，本次是从kafka再次接收到该消息。需要继续走后续处理流程</br>
  如果插入成功，则继续走后续处理流程。</br>
  <b>后续处理流程</b>： 调用API接口向用户推送消息。然后更新DB中该消息状态为已推送。最后提交kafka消息offset。
  </pre>
  ### 其他
  - 这里注意了优雅关闭处理。</br>
  - 另外，生产者和消费者可以基于campaign.ID进行独立处理，比如生产者进程1专门处理campaign.ID % 3 == 0的活动并推送给分区0，生产者进程2专门处理campaign.ID % 3 == 1的活动并推送给分区1..., </br>
  - 同样的，也可以通过扩容kafka分区数量，同时增加消费者进程数量或者增加worker_count启动参数，来增加并发消费能力。</br>
  - 如果活动量比较大，可能数据库也需要考虑基于活动ID进行分库分表。








