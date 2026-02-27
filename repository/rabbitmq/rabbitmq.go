package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CocaineCong/gin-mall/pkg/utils/log"
	"github.com/CocaineCong/gin-mall/repository/db/dao"
	"github.com/CocaineCong/gin-mall/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func SendMessage(ctx context.Context, rabbitMqQueue string, message []byte) error {
	ch, err := GlobalRabbitMQ.Channel()

	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			return
		}
	}(ch)

	q, err := ch.QueueDeclare(rabbitMqQueue, true, false, false, false, nil)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(ctx, "", q.Name, false, false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         message},
	)
	return err
}

func ConsumeMessage(ctx context.Context, rabbitMqQueue string) (<-chan amqp.Delivery, error) {
	ch, err := GlobalRabbitMQ.Channel()
	if err != nil {
		fmt.Println("err", err)
		return nil, err
	}
	q, _ := ch.QueueDeclare(rabbitMqQueue, true, false, false, false, nil)
	return ch.Consume(q.Name, "", true, false, false, false, nil)
}

// TODO: 将这里的交换机名称和队列等名称都放到配置文件中
func PublishSkillMessage(ctx context.Context, msg string) error {
	ch, err := GlobalRabbitMQ.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	err = ch.ExchangeDeclare(
		"skill",
		"direct",
		true,
		false,
		false,
		false,
		nil)

	if err != nil {
		return err
	}

	// 生产端其实不用创建队列，队列是消费端在用
	_, err = ch.QueueDeclare(
		"stock_deduct_queue",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(),
		"skill",
		"skill.product", // 路由键，由消费端绑定
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		},
	)

	if err != nil {
		return err
	}

	return nil
}

// 启动消费者，监听扣库存队列
func StartDeductStockConsumer() error {

	ch, err := GlobalRabbitMQ.Channel()
	if err != nil {
		log.LogrusObj.Error("create channel failed: %v", err)
		return err
	}

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {

		}
	}(ch)

	err = ch.ExchangeDeclare(
		"skill",
		"direct",
		true,
		false,
		false,
		false,
		nil)

	if err != nil {
		return err
	}

	// 声明队列（幂等操作，已存在则无影响）
	_, err = ch.QueueDeclare(
		"stock_deduct_queue",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	// 绑定队列到交换机，指定绑定键（和生产端路由键一致）
	err = ch.QueueBind(
		"stock_deduct_queue", // 队列名
		"skill.product",      // 绑定键（必须和生产端路由键完全一致）
		"skill",              // 交换机名
		false,
		nil,
	)

	if err != nil {
		return err
	}

	// 限制每次只取1条消息，处理完再取（防止过载）
	err = ch.Qos(
		1,     // 每次预取1条
		0,     // 全局限制
		false, // 非共享
	)
	if err != nil {
		log.LogrusObj.Error("set qos failed: %v", err)
	}

	//绑定队列到交换器
	err = ch.QueueBind(
		"stock_deduct_queue",
		"skill.product",
		"skill",
		false,
		nil,
	)

	if err != nil {
		log.LogrusObj.Error("bind queue failed: %v", err)
	}

	// 消费消息
	msgs, err := ch.Consume(
		"stock_deduct_queue", // 队列名
		"",                   // 消费者标签
		false,                // 关闭自动确认
		false,                // 不排他
		false,                // 不阻塞
		false,                // 无参数
		nil,
	)
	if err != nil {
		log.LogrusObj.Error("consume failed: %v", err)
	}

	// 循环处理消息
	for msg := range msgs {
		// 解析消息
		var deductMsg types.ProductMQ
		err := json.Unmarshal(msg.Body, &deductMsg)
		if err != nil {
			log.LogrusObj.Error("unmarshal msg failed: %v", err)
			err := msg.Nack(false, false)
			if err != nil {
				log.LogrusObj.Error("msg Nack failed: %v", err)
				return err
			}
			continue
		}

		// 处理扣减MySQL库存
		err = productMySQLStock(context.TODO(), deductMsg.ProductId)
		if err != nil {
			log.LogrusObj.Error("deduct stock failed: productId=%d, err=%v", deductMsg.ProductId, err)
			err = msg.Nack(false, true)
			if err != nil {
				log.LogrusObj.Error("msg Nack failed: %v", err)
				return err
			} // 处理失败，重新入队重试
			continue
		}

		// 处理成功，手动确认消息
		err = msg.Ack(false)
		if err != nil {
			log.LogrusObj.Error("msg Nack failed: %v", err)
			return err
		}
		log.LogrusObj.Error("deduct stock success: productId=%d", deductMsg.ProductId)
	}
	return nil
}

// 扣减MySQL库存（核心逻辑）
func productMySQLStock(ctx context.Context, productId uint) error {
	return dao.NewSkillGoodsDao(ctx).SkillGoods(productId)
}
