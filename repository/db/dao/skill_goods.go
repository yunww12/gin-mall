package dao

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/CocaineCong/gin-mall/consts"
	"github.com/CocaineCong/gin-mall/repository/db/model"
)

type SkillGoodsDao struct {
	*gorm.DB
}

func NewSkillGoodsDao(ctx context.Context) *SkillGoodsDao {
	return &SkillGoodsDao{NewDBClient(ctx)}
}

func (dao *SkillGoodsDao) Create(in *model.SkillProduct) error {
	return dao.Model(&model.SkillProduct{}).Create(&in).Error
}

func (dao *SkillGoodsDao) BatchCreate(in []*model.SkillProduct) error {
	return dao.Model(&model.SkillProduct{}).
		CreateInBatches(&in, consts.ProductBatchCreate).Error
}

func (dao *SkillGoodsDao) CreateByList(in []*model.SkillProduct) error {
	return dao.Model(&model.SkillProduct{}).Create(&in).Error
}

func (dao *SkillGoodsDao) ListSkillGoods() (resp []*model.SkillProduct, err error) {
	err = dao.Model(&model.SkillProduct{}).
		Where("num > 0").Find(&resp).Error

	return
}

// 库存扣减，使用事务保证数据一致性
// TODO:结合订单系统+支付系统
// 秒杀成功后，先创建订单，支付成功后才能扣减
// 暂时直接扣减库存
// 考虑批量处理RabbitMQ中的消息，减少mysql操作次数，提高性能
func (dao *SkillGoodsDao) SkillGoods(id uint) (err error) {
	// 核心：用GORM的Expr实现 num = num - 1，Where同时限定id和num > 0
	result := dao.Model(&model.SkillProduct{}).
		Where("product_id = ? AND num > 0", id).     // 匹配id且库存大于0
		UpdateColumn("num", gorm.Expr("num - ?", 1)) // 表达式更新，扣减1个库存

	// 可选：检查是否真的更新了数据（避免库存不足时无更新但不报错）
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("库存不足或商品不存在，id: %d", id)
	}
	return nil
}
