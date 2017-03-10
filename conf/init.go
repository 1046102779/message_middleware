package conf

import (
	"fmt"
	"strings"
	"time"

	"github.com/1046102779/common/utils"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/orm"
	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/clientselector"
	"github.com/smallnest/rpcx/codec"
)

var (
	RpcAddr, EtcdAddr     string
	Topics                []string
	KafkaClusterServers   []string
	Servers               []string
	OfficialAccountClient *rpcx.Client
	SmsClient             *rpcx.Client

	DBDebug bool
)

func initMQ() {
	KafkaClusterServers = beego.AppConfig.Strings("kafka-cluster::servers")
	if KafkaClusterServers == nil || len(KafkaClusterServers) <= 0 {
		panic("param `kafka-cluster::servers` empty")
	}
	Topics = beego.AppConfig.Strings("kafka-cluster::topics")
	if Topics == nil || len(Topics) <= 0 {
		panic("param `kafka-cluster::topics` empty")
	}
}

func initEtcdClient() {
	RpcAddr = strings.TrimSpace(beego.AppConfig.String("rpc::address"))
	EtcdAddr = strings.TrimSpace(beego.AppConfig.String("etcd::address"))
	if "" == EtcdAddr || "" == RpcAddr {
		panic("param `etcd::address || etcd::address` empty")
	}
	serverTemp := beego.AppConfig.String("rpc::servers")
	Servers = strings.Split(serverTemp, ",")
}

func connRpcClient(appName string) (client *rpcx.Client) {
	s := clientselector.NewEtcdClientSelector([]string{EtcdAddr}, fmt.Sprintf("/%s/%s/%s", beego.BConfig.RunMode, "rpcx", appName), time.Minute, rpcx.RandomSelect, time.Minute)
	client = rpcx.NewClient(s)
	client.FailMode = rpcx.Failover
	client.ClientCodecFunc = codec.NewProtobufClientCodec
	return
}

func init() {
	var (
		name  string
		exist bool = false
		err   error
	)
	// 初始化kafka队列消费者
	initMQ()

	// 初始化Etcd客户端连接
	initEtcdClient()

	if name, exist = utils.FindServer("official_accounts", Servers); !exist {
		panic("param `official_account` service not exist")
	}
	OfficialAccountClient = connRpcClient(name)
	if name, exist = utils.FindServer("sms", Servers); !exist {
		panic("param `sms` service not exist")
	}
	SmsClient = connRpcClient(name)

	// orm debug
	DBDebug, err = beego.AppConfig.Bool("dev::debug")
	if err != nil {
		panic("app parameter `dev::debug` error:" + err.Error())
	}
	if DBDebug {
		orm.Debug = true
	}
}
