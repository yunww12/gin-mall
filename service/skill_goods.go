package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"strconv"
	"sync"
	"time"

	"github.com/CocaineCong/gin-mall/pkg/utils/log"
	"github.com/CocaineCong/gin-mall/repository/cache"
	"github.com/CocaineCong/gin-mall/repository/db/dao"
	"github.com/CocaineCong/gin-mall/repository/db/model"
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

// 这里使用了一个string key和一个set key实现redis中秒杀商品的管理，这样好吗
// 能否只使用一个key
// set key是用来管理所有商品ID，便于展示所有商品的，但是好像没必要？因为商品ID是唯一的？
func syncPoructInfoToRedis(ctx context.Context, spList []*model.SkillProduct) error {
	var (
		setKeys []string      // 存储所有商品详情的Redis Key
		setVals []interface{} // 存储所有商品详情的JSON字符串
		idStrs  []interface{} // 存储所有秒杀商品ID（用于SADD）
		hasErr  bool          // 标记是否出现本地错误
	)

	// 步骤1：提前遍历组装所有需要的Key、JSON串、商品ID，做本地校验
	for _, sp := range spList {
		// 1. 序列化商品信息，本地校验失败直接返回
		jsonBytes, err := json.Marshal(sp)
		if err != nil {
			log.LogrusObj.Infof("商品ID:%d 序列化失败: %v", sp.ProductId, err)
			return err
		}
		jsonString := string(jsonBytes)

		// 2. 组装String类型的Set命令参数（Key+Value）
		keyStr := fmt.Sprintf(cache.SkillProductKey, sp.ProductId)
		setKeys = append(setKeys, keyStr)
		setVals = append(setVals, jsonString)

		// 3. 组装Set集合的SADD命令参数（商品ID）
		idStr := strconv.Itoa(int(sp.ProductId))
		idStrs = append(idStrs, idStr)
	}

	// 步骤2：一次TxPipelined执行所有命令，仅1次Redis网络请求
	// 原子性保障：单商品的Set + SADD 事务封装（推荐生产环境使用）
	// 若要求「商品详情缓存」和「ID集合添加」必须同时成功/失败，使用以下事务逻辑
	if len(setKeys) > 0 && !hasErr {
		_, err := cache.RedisClient.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			// 批量入队所有商品详情的SET命令（循环入队，无额外网络请求）
			for i := range setKeys {
				pipe.Set(ctx, setKeys[i], setVals[i], 1*time.Hour)
			}
			// 一次入队所有商品ID的SADD命令（支持多参数，天然去重）
			pipe.SAdd(ctx, cache.SkillProductSetKey, idStrs...)
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
			Money:     200,
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

// ListSkillGoods 列表展示
func (s *SkillProductSrv) ListSkillGoods(ctx context.Context) (resp interface{}, err error) {
	// 读缓存
	rc := cache.RedisClient
	// 获取列表
	skillProductList, err := rc.SMembers(ctx, cache.SkillProductSetKey).Result()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	// 这里需要考虑到多个客户端都没有从redis中查询到商品信息的情况
	if len(skillProductList) == 0 {
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
			skillProductList, err = rc.SMembers(ctx, cache.SkillProductSetKey).Result()
			if err != nil {
				log.LogrusObj.Infoln(err)
				return nil, err
			}
			if len(skillProductList) >= 0 {
				// 查询redis，将ID转换为商品信息
				productJsons := make([]string, 0)
				for _, str := range skillProductList {
					p, errx := rc.Get(ctx, str).Result()
					if errx == nil {
						productJsons = append(productJsons, p)
					}
				}
				resp = productJsons
				return resp, nil
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
			skillProductList, err = rc.SMembers(ctx, cache.SkillProductSetKey).Result()
			if err == nil && len(skillProductList) > 0 {
				productJsons := make([]string, 0, len(skillProductList))
				for _, key := range skillProductList {
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
		productJsons := make([]string, 0)
		for _, str := range skillProductList {
			p, errx := rc.Get(ctx, str).Result()
			if errx == nil {
				productJsons = append(productJsons, p)
			}
		}
		resp = productJsons
	}

	return
}

// GetSkillGoods 详情展示
func (s *SkillProductSrv) GetSkillGoods(ctx context.Context, req *types.GetSkillProductReq) (resp interface{}, err error) {
	// 读缓存
	rc := cache.RedisClient
	// 获取列表
	resp, err = rc.Get(ctx,
		fmt.Sprintf(cache.SkillProductKey, req.ProductId)).Result()
	if err != nil {
		//log.LogrusObj.Infoln(err)
		return
	}

	return
}

// SkillProduct 秒杀商品
func (s *SkillProductSrv) SkillProduct(ctx context.Context, req *types.SkillProductReq) (resp interface{}, err error) {
	// 读缓存
	rc := cache.RedisClient
	// 获取数据
	// 这里有个问题：在秒杀商品时，是将商品数量减一
	// 这里需要同时更新redis和mysql中的数据，是不是太复杂了，有没有更好的方式？
	resp, err = rc.Get(ctx,
		fmt.Sprintf(cache.SkillProductKey, req.ProductId)).Result()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	return
}

//SkillProductMQ2MySQL 从mq落库
//func SkillProductMQ2MySQL(ctx context.Context, req *story_types.LikeStoryReq) (err error) {
//	storyDao := dao.NewStoryDao(ctx)
//	usDao := dao.NewUserStoryDao(ctx)
//	err = storyDao.UpdateStoryLikeOrStar(req.StoryId, 1, false)
//	if err != nil {
//		log.LogrusObj.Infoln(err)
//		return
//	}
//
//	err = usDao.UserStoryUpsert(&user_story_types.UserStoryReq{
//		UserId:        req.UserId,
//		StoryId:       req.StoryId,
//		OperationType: user_story_consts.UserStoryOperationTypeLike,
//	})
//	if err != nil {
//		log.LogrusObj.Infoln(err)
//		return
//	}
//
//	return
//}
