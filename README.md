## 消息发布-订阅中间件服务
    为了促进产品的平台化建设, 把kafka消息队列从服务中抽象出来。让消息队列只关注消息的订阅和发布，使用微服务，减少与业务逻辑的耦合, 该服务支持多实例部署

    使用方法：只需要在models/grpc_server.go文件中填充要接收的消息体和发送的消息体protocolbuffer，业务逻辑在其他业务逻辑服务中实现, 本项目写了一个与工作有关的订单和短信订阅和发送实例, 其目的是让大家明白怎么样使用该服务。

## 技术栈
1. [**beego**](https://beego.me/)
2. [**rpcx**](github.com/smallnest/rpcx)
3. [**sarama**](https://github.com/Shopify/sarama)`

Standard  `go get`:

```go
$  go get -v -u github.com/1046102779/message_middleware
```



## 说明

+ `希望与大家一起成长，有任何该服务运行或者代码问题，可以及时找我沟通，喜欢开源，热爱开源, 欢迎多交流`   
+ `联系方式：cdh_cjx@163.com`
