package main

import (
	"fmt"
	"time"

	"github.com/1046102779/message_middleware/conf"
	"github.com/1046102779/message_middleware/libs"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/codec"
	"github.com/smallnest/rpcx/plugin"
)

func startRPCService(rpcAddr string, etcdAddr string, producerConsumerServer *libs.ProducerConsumerServer) {
	server := rpcx.NewServer()
	rplugin := &plugin.EtcdRegisterPlugin{
		ServiceAddress: "tcp@" + rpcAddr,
		EtcdServers:    []string{etcdAddr},
		BasePath:       fmt.Sprintf("/%s/%s", conf.Cconfig.RunMode, "rpcx"),
		Metrics:        metrics.NewRegistry(),
		Services:       make([]string, 0),
		UpdateInterval: time.Minute,
	}
	rplugin.Start()
	server.PluginContainer.Add(rplugin)
	server.PluginContainer.Add(plugin.NewMetricsPlugin())
	server.RegisterName("message_middlewares", producerConsumerServer, "weight=1&m=devops")
	server.ServerCodecFunc = codec.NewProtobufServerCodec
	server.Serve("tcp", rpcAddr)
}

func main() {
	fmt.Println("main starting...")
	startRPCService(conf.RpcAddr, conf.EtcdAddr, &libs.ProducerConsumerServer{})
}
