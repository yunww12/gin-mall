package v1

import (
	"encoding/json"
	"fmt"
	"github.com/CocaineCong/gin-mall/repository/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/CocaineCong/gin-mall/pkg/utils/ctl"
	"github.com/CocaineCong/gin-mall/pkg/utils/log"
	"github.com/CocaineCong/gin-mall/service"
	"github.com/CocaineCong/gin-mall/types"
)

// InitSkillProductHandler 初始化秒杀商品信息
func InitSkillProductHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var req types.ListSkillProductReq
		if err := ctx.ShouldBind(&req); err != nil {
			// 参数校验
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}

		l := service.GetSkillProductSrv()
		resp, err := l.InitSkillGoods(ctx.Request.Context())
		if err != nil {
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}
		ctx.JSON(http.StatusOK, ctl.RespSuccess(ctx, resp))
	}
}

// ListSkillProductHandler 初始化秒杀商品信息
func ListSkillProductHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var req types.ListSkillProductReq
		if err := ctx.ShouldBind(&req); err != nil {
			// 参数校验
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}

		l := service.GetSkillProductSrv()
		resp, err := l.ListSkillGoods(ctx.Request.Context())
		if err != nil {
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}
		ctx.JSON(http.StatusOK, ctl.RespSuccess(ctx, resp))
	}
}

// GetSkillProductHandler 获取秒杀商品的详情
func GetSkillProductHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var req types.GetSkillProductReq
		fmt.Println("GetSkillProductHandler")
		if err := ctx.ShouldBind(&req); err != nil {
			// 参数校验
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}

		l := service.GetSkillProductSrv()
		fmt.Println("GetSkillProductHandler1")

		resp, err := l.GetSkillGoods(ctx.Request.Context(), &req)
		fmt.Println("GetSkillProductHandler2")

		if err != nil {
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}
		ctx.JSON(http.StatusOK, ctl.RespSuccess(ctx, resp))
	}
}

func SkillProductHandler() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var req types.SkillProductReq
		if err := ctx.ShouldBind(&req); err != nil {
			// 参数校验
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}

		l := service.GetSkillProductSrv()
		resp, err := l.SkillProduct(ctx.Request.Context(), &req)
		if err != nil {
			log.LogrusObj.Infoln(err)
			ctx.JSON(http.StatusOK, ErrorResponse(ctx, err))
			return
		}
		ctx.JSON(http.StatusOK, ctl.RespSuccess(ctx, resp))
	}
}

// 启动消费者，监听扣库存队列
func StartDeductStockConsumer() {

	ch, err := rabbitmq.GlobalRabbitMQ.Channel()
	if err != nil {
		log.LogrusObj.Error("create channel failed: %v", err)
	}

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {

		}
	}(ch)

	// 限制每次只取1条消息，处理完再取（防止过载）
	err = ch.Qos(
		1,     // 每次预取1条
		0,     // 全局限制
		false, // 非共享
	)
	if err != nil {
		log.LogrusObj.Error("set qos failed: %v", err)
	}

	// 消费消息
	msgs, err := ch.Consume(
		"skill_deduct_stock_queue", // 队列名
		"",                         // 消费者标签
		false,                      // 关闭自动确认
		false,                      // 不排他
		false,                      // 不阻塞
		false,                      // 无参数
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
				return
			}
			continue
		}

		// 处理扣减MySQL库存
		err = productMySQLStock(deductMsg.ProductId)
		if err != nil {
			log.LogrusObj.Error("deduct stock failed: productId=%d, err=%v", deductMsg.ProductId, err)
			err = msg.Nack(false, true)
			if err != nil {
				log.LogrusObj.Error("msg Nack failed: %v", err)
				return
			} // 处理失败，重新入队重试
			continue
		}

		// 处理成功，手动确认消息
		err = msg.Ack(false)
		if err != nil {
			log.LogrusObj.Error("msg Nack failed: %v", err)
			return
		}
		log.LogrusObj.Error("deduct stock success: productId=%d", deductMsg.ProductId)
	}
}

// 扣减MySQL库存（核心逻辑）
func productMySQLStock(productId uint) error {
	sql := `UPDATE skill_product SET stock = stock - 1 WHERE id = ? AND stock > 0`
	result := db.Exec(sql, productId)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("stock insufficient")
	}
	return nil
}
