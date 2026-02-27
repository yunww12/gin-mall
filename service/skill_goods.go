package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/CocaineCong/gin-mall/pkg/utils/log"
	"github.com/CocaineCong/gin-mall/repository/cache"
	"github.com/CocaineCong/gin-mall/repository/db/dao"
	"github.com/CocaineCong/gin-mall/repository/db/model"
	"github.com/CocaineCong/gin-mall/repository/rabbitmq"
	"github.com/CocaineCong/gin-mall/types"
)

var SkillProductSrvIns *SkillProductSrv
var SkillProductSrvOnce sync.Once

type SkillProductSrv struct {
}

func GetSkillProductSrv() *SkillProductSrv {
	SkillProductSrvOnce.Do(func() {
		SkillProductSrvIns = &SkillProductSrv{}
	})
	return SkillProductSrvIns
}

// 使用集合（Set）存储所有商品ID，具有如下优势:
// 1.遍历所有商品或者批量遍历商品时，调用RedisClient.SIsMember速度更快，而如果是遍历string或者list类型的缓存较慢
// 2.用于检查商品ID的合法性，避免用户恶意操作，比如传入一个不存在的商品ID

// 使用hash类型存储商品信息相比于使用json字符串存储商品信息具有如下优势
// 1.可以直接对hash类型商品信息中的字段进行递增，递减操作，更加适用于库存扣减
// 2.
func syncPoructInfoToRedis(ctx context.Context, skillProductList []*model.SkillProduct) error {
	var (
		productHashKeys   []string                                  // 存储所有商品详情的Redis Key
		productHashFields = make(map[string]map[string]interface{}) // 以hash类型存储的所有商品详情
		productIdStrs     []interface{}                             // 存储所有秒杀商品ID（用于SADD）
	)

	// 步骤1：提前遍历组装所有需要的Key、商品ID，做本地校验
	for _, skillProduct := range skillProductList {

		// 2. 组装String类型的Set命令参数（Key+Value）
		productHashKey := fmt.Sprintf(cache.SkillProductKey, skillProduct.ProductId)
		productHashKeys = append(productHashKeys, productHashKey)
		productHashFields[productHashKey] = map[string]interface{}{
			"stock":      skillProduct.Num,
			"price":      skillProduct.Price,
			"product_id": skillProduct.Id,
			"title":      skillProduct.Title,
			"boss_id":    skillProduct.BossId,
		}

		// 3. 组装Set集合的SADD命令参数（商品ID）
		productIdStr := strconv.Itoa(int(skillProduct.ProductId))
		productIdStrs = append(productIdStrs, productIdStr)
	}

	// 步骤2：一次TxPipelined执行所有命令，仅1次Redis网络请求
	// 原子性保障：单商品的Hash + SADD 事务封装（推荐生产环境使用）
	// 若要求「商品详情缓存」和「ID集合添加」必须同时成功/失败，使用以下事务逻辑
	if len(productHashKeys) > 0 {
		_, err := cache.RedisClient.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			// 批量入队所有商品详情（循环入队，无额外网络请求）
			for _, key := range productHashKeys {
				fields := productHashFields[key]
				pipe.HMSet(ctx, key, fields)
				pipe.Expire(ctx, key, 1*time.Hour) //设置过期时间
			}
			// 一次入队所有商品ID的SADD命令（支持多参数，天然去重）
			pipe.SAdd(ctx, cache.SkillProductSetKey, productIdStrs...)
			return nil
		})

		if err != nil {
			log.LogrusObj.Infof("秒杀商品缓存批量事务执行失败: %v", err)
			return err
		}
	}
	return nil
}

// InitSkillGoods 初始化商品信息
func (s *SkillProductSrv) InitSkillGoods(ctx context.Context) (resp interface{}, err error) {
	spList := make([]*model.SkillProduct, 0)
	for i := 1; i < 10; i++ {
		spList = append(spList, &model.SkillProduct{
			ProductId: uint(i),
			BossId:    2,
			Title:     "秒杀商品测试使用",
			Price:     200,
			Num:       10,
		})
	}

	// 商品信息同步到mysql
	err = dao.NewSkillGoodsDao(ctx).BatchCreate(spList)
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	// 导入数据库的同时，初始化缓存
	err = syncPoructInfoToRedis(ctx, spList)
	return
}

// releaseLock 释放分布式锁（Lua脚本保证原子性：校验value+删除锁）
func releaseLock(ctx context.Context, rc *redis.Client, lockKey, lockValue string) {
	// Lua脚本：先判断锁的value是否一致，一致则删除
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	// 执行脚本
	_, err := rc.Eval(ctx, script, []string{lockKey}, lockValue).Result()
	if err != nil {
		log.LogrusObj.Errorf("释放分布式锁失败: %v, key: %s", err, lockKey)
	}
}

func productFieldsToSkillProduct(productFields map[string]string) *model.SkillProduct {
	skillProduct := &model.SkillProduct{}

	var (
		val string
		ok  bool = true
	)

	val, ok = productFields["product_id"]
	if !ok {
		return nil
	}
	id, _ := strconv.ParseUint(val, 10, 64)
	skillProduct.ProductId = uint(id)

	val, ok = productFields["price"]
	if !ok {
		return nil
	}
	price, _ := strconv.ParseFloat(val, 64)
	skillProduct.Price = price

	val, ok = productFields["boss_id"]
	if !ok {
		return nil
	}
	bossId, _ := strconv.ParseUint(val, 10, 64)
	skillProduct.BossId = uint(bossId)

	val, ok = productFields["title"]
	if !ok {
		return nil
	}
	skillProduct.Title = val

	return skillProduct
}

// 根据商品ID列表查询商品信息
func ListSkillGoodsByIDs(ctx context.Context, skillProductIdStrList []string) (resp interface{}, err error) {
	rc := cache.RedisClient
	skillProducts := []*model.SkillProduct{}
	skillProductsJsons := []string{}
	for _, productIdStr := range skillProductIdStrList {
		productHashKey := fmt.Sprintf(cache.SkillProductKey, productIdStr)
		productFields, errx := rc.HGetAll(ctx, productHashKey).Result()

		if errx == nil {
			skillProduct := productFieldsToSkillProduct(productFields)
			if skillProduct != nil {
				skillProducts = append(skillProducts, skillProduct)
				productJson, err := json.Marshal(*skillProduct)
				if err != nil {
					log.LogrusObj.Error("json Marshal Error: %v", err)
					return nil, err
				}
				skillProductsJsons = append(skillProductsJsons, string(productJson))
			}
		}
	}
	resp = skillProductsJsons
	return resp, nil
}

// 根据指定商品ID查询商品信息
func ListSkillGoodsByID(ctx context.Context, productIdStr string) (resp interface{}, err error) {
	rc := cache.RedisClient

	productHashKey := fmt.Sprintf(cache.SkillProductKey, productIdStr)
	productFields, errx := rc.HGetAll(ctx, productHashKey).Result()

	if errx == nil {
		skillProduct := productFieldsToSkillProduct(productFields)
		if skillProduct != nil {
			productJson, err := json.Marshal(*skillProduct)
			if err != nil {
				log.LogrusObj.Error("json Marshal Error: %v", err)
				return nil, err
			}
			resp = productJson
		}
	}
	return resp, nil
}

// 根据指定商品ID查询商品信息
// TODO: 需要和上面的函数合并，功能重复了
func GetSkillGoodsByID(ctx context.Context, productIdStr string) (product *model.SkillProduct, err error) {
	rc := cache.RedisClient

	productHashKey := fmt.Sprintf(cache.SkillProductKey, productIdStr)
	productFields, errx := rc.HGetAll(ctx, productHashKey).Result()

	if errx == nil {
		product := productFieldsToSkillProduct(productFields)
		if product != nil {
			return product, nil
		}
	}
	return product, errx
}

// ListSkillGoods 列表展示
func (s *SkillProductSrv) ListSkillGoods(ctx context.Context) (resp interface{}, err error) {
	// 读缓存
	rc := cache.RedisClient
	// 获取列表
	skillProductIdStrList, err := rc.SMembers(ctx, cache.SkillProductSetKey).Result()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	// 这里需要考虑到多个客户端都没有从redis中查询到商品信息的情况
	if len(skillProductIdStrList) == 0 {
		lockKey := "lock:skill:product:list"
		lockValue := uuid.NewString()
		expiredTime := 1 * time.Second
		// 如果已经上锁，那么locked为false，errs为nil
		locked, errs := rc.SetNX(ctx, lockKey, lockValue, expiredTime).Result()
		if errs != nil {
			log.LogrusObj.Infoln(err)
			return nil, errs
		}

		if locked {
			defer releaseLock(ctx, rc, lockKey, lockValue)
			// double check：很重要，防止其他协程刚访问mysql成功，这里又成功获取到锁的情况
			skillProductIdStrList, err = rc.SMembers(ctx, cache.SkillProductSetKey).Result()
			if err != nil {
				log.LogrusObj.Infoln(err)
				return nil, err
			}
			if len(skillProductIdStrList) >= 0 {
				// 查询redis，将ID转换为商品信息
				return ListSkillGoodsByIDs(ctx, skillProductIdStrList)
			}

			skill, errs := dao.NewSkillGoodsDao(ctx).ListSkillGoods()
			if errs != nil {
				log.LogrusObj.Infoln(errs)
				return nil, errs
			}

			// 需要使用分布式锁
			err = syncPoructInfoToRedis(ctx, skill)
			if err != nil {
				log.LogrusObj.Infoln(err)
				return nil, err
			}
			resp = skill
			return resp, nil
		}

		// 未获取到锁：短暂等待后重试读缓存（避免无限循环，可设置重试次数）
		// 未获取到锁说明有其他协程在访问数据库
		// 重试次数+间隔，避免频繁请求Redis
		retryTimes := 3
		retryInterval := 100 * time.Millisecond
		for i := 0; i < retryTimes; i++ {
			time.Sleep(retryInterval)
			// 重试读缓存
			skillProductIdStrList, err = rc.SMembers(ctx, cache.SkillProductSetKey).Result()
			if err == nil && len(skillProductIdStrList) > 0 {
				productJsons := make([]string, 0, len(skillProductIdStrList))
				for _, key := range skillProductIdStrList {
					p, _ := rc.Get(ctx, key).Result()
					productJsons = append(productJsons, p)
				}
				return productJsons, nil
			}
		}

		// 重试后仍无缓存，返回空或提示（根据业务调整）
		log.LogrusObj.Warn("重试后仍未获取到秒杀商品缓存")
		return nil, nil

	} else {
		// 查询redis，将ID转换为商品信息
		return ListSkillGoodsByIDs(ctx, skillProductIdStrList)
	}
}

// GetSkillGoods 详情展示
func (s *SkillProductSrv) GetSkillGoods(ctx context.Context, req *types.GetSkillProductReq) (resp interface{}, err error) {
	return ListSkillGoodsByID(ctx, string(req.ProductId))
}

// SkillProduct 秒杀商品
// 作为RabbitMQ的生产端
// 如何保证接口是从客户端调用的，避免接口被滥用
// TODO: 秒杀数量需要支持大于1
func (s *SkillProductSrv) SkillProduct(ctx context.Context, req *types.SkillProductReq) (resp interface{}, err error) {
	// 1.首先查询商品ID是否在集合skill:product:ids内
	rc := cache.RedisClient
	exist, err := rc.SIsMember(ctx, cache.SkillProductSetKey, string(req.ProductId)).Result()
	if err != nil {
		// TODO: 返回错误信息resp
		return nil, err
	}

	if !exist {
		// TODO: 返回错误信息resp
		return nil, nil
	}

	// 下面这种写法不行，无法保证库存原子性，应该利用redis的递增递减操作原子扣减库存
	// 从redis查询库存
	// product, err := GetSkillGoodsByID(ctx, string(req.ProductId))
	// if err != nil {
	// 	return nil, err
	// }
	// if product.Num > 0 {

	// }

	// 库存预扣减
	productKey := fmt.Sprintf(cache.SkillProductKey, string(req.ProductId))

	const preDeductStockLua = `
		local stock = tonumber(redis.call('HGET', KEYS[1], 'stock'))
		local deductNum = tonumber(ARGV[1])

		if not stock or stock < deductNum then
    		return -1  -- 库存不足，返回0
		end

		-- 库存充足，扣减并返回新库存
		redis.call('HINCRBY', KEYS[1], 'stock', -deductNum)
		return stock - deductNum
`
	result, err := rc.Eval(ctx, preDeductStockLua, []string{productKey}, 1).Int64()
	if result < 0 {
		err = errors.New("库存不足")
		return
	}

	// 库存扣减成功，将消息写入RabbitMQ，立即返回成功消息
	err = rabbitmq.PublishSkillMessage(ctx, string(req.ProductId))

	if err != nil {
		return nil, err
	}

	// TODO：通知订单系统
	return "秒杀成功", nil
}
