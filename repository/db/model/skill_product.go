package model

type SkillProduct struct {
	Id         uint `gorm:"primarykey"`
	ProductId  uint `gorm:"not null"`
	BossId     uint `gorm:"not null"`
	Title      string
	Price      float64
	Num        int `gorm:"not null"`
	CustomId   uint
	CustomName string
}

type SkillProductMQMsg struct {
	ProductId uint `json:"product_id"`
}
