package dao

import (
	"context"

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
// TODO:结合订单系统
func (dao *SkillGoodsDao) SkillGoods() (productId uint, num uint, err error) {

}
