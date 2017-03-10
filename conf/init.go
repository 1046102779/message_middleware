package conf

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/astaxie/beego/config"
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
	Cconfig               *GlobalConfig = new(GlobalConfig)
)

type GlobalConfig struct {
	AppName string
	RunMode string
}

func initConfig() {
	iniconf, err := config.NewConfig("ini", "conf/app.conf")
	if err != nil {
		log.Fatal(err)
	}
	// global config
	Cconfig.AppName = iniconf.String("appname")
	Cconfig.RunMode = iniconf.String("runmode")
	if Cconfig.AppName == "" || Cconfig.RunMode == "" {
		panic("param `global config` empty")
	}

	// kafka-cluster config
	KafkaClusterServers = iniconf.Strings("kafka-cluster::servers")
	if KafkaClusterServers == nil || len(KafkaClusterServers) <= 0 {
		panic("param `kafka-cluster::servers` empty")
	}
	Topics = iniconf.Strings("kafka-cluster::topics")
	if Topics == nil || len(Topics) <= 0 {
		panic("param `kafka-cluster::topics` empty")
	}

	// rpc config
	RpcAddr = iniconf.String("rpc::address")
	Servers = iniconf.Strings("rpc::servers")
	if RpcAddr == "" || Servers == nil || len(Servers) <= 0 {
		panic("param `rpc::address || rpc::servers`  empty")
	}

	// etcd config
	EtcdAddr = iniconf.String("etcd::address")
	if EtcdAddr == "" {
		panic("param `etcd config` empty")
	}

	return
}

func connRpcClient(appName string) (client *rpcx.Client) {
	s := clientselector.NewEtcdClientSelector([]string{EtcdAddr}, fmt.Sprintf("/%s/%s/%s", Cconfig.RunMode, "rpcx", appName), time.Minute, rpcx.RandomSelect, time.Minute)
	client = rpcx.NewClient(s)
	client.FailMode = rpcx.Failover
	client.ClientCodecFunc = codec.NewProtobufClientCodec
	return
}

func FindServer(server string, servers []string) (name string, exist bool) {
	if servers == nil || len(servers) <= 0 || server == "" {
		return "", false
	}
	for index := 0; index < len(servers); index++ {
		if strings.HasPrefix(servers[index], server) {
			return servers[index], true
		}
	}
	return "", false
}

func init() {
	var (
		name  string
		exist bool = false
	)
	initConfig()

	if name, exist = FindServer("official_accounts", Servers); !exist {
		panic("param `official_account` service not exist")
	}
	OfficialAccountClient = connRpcClient(name)
	if name, exist = FindServer("sms", Servers); !exist {
		panic("param `sms` service not exist")
	}
	SmsClient = connRpcClient(name)
}
