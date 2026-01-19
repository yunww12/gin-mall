package es

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"

	conf "github.com/CocaineCong/gin-mall/config"
	"github.com/CocaineCong/gin-mall/repository/db/model"
)

var EsClient *elastic.TypedClient
var EsClientLog *elastic.Client

// InitEs 初始化es
func InitEs() {
	eConfig := conf.Config.Es
	esConn := fmt.Sprintf("https://%s:%s", eConfig.EsHost, eConfig.EsPort)
	cfg := elastic.Config{
		Addresses: []string{esConn},
		Username:  conf.Config.Es.UserName,
		Password:  conf.Config.Es.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // 关闭证书校验
			},
		},
	}
	client, err := elastic.NewTypedClient(cfg)

	if err != nil {
		log.Panic(err)
	}

	EsClientLog, err = elastic.NewClient(cfg)

	if err != nil {
		log.Panic(err)
	}

	EsClient = client
	createProductIndex()
}

// EsHookLog 初始化log日志
// func EsHookLog() *eslogrus.ElasticHook {
// 	eConfig := conf.Config.Es
// 	hook, err := eslogrus.NewElasticHook(EsClientLog, eConfig.EsHost, logrus.DebugLevel, eConfig.EsIndex)
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	return hook
// }

func SyncProductInfoToEs(product *model.Product) {
	esProduct := &model.EsProduct{
		ID:    product.ID,
		Name:  product.Name,
		Title: product.Title,
		Info:  product.Info,
	}
	fmt.Println("========", conf.Config.Es.ProductIndex)
	resp, err := EsClient.
		Index(conf.Config.Es.ProductIndex).
		Id(strconv.FormatInt(int64(esProduct.ID), 10)).
		Document(esProduct).Do(context.Background())
	if err != nil {
		fmt.Println("======sync product to es failed, err:", err)
	} else {
		fmt.Println(resp.Result)
	}
}

func SearchProductFromEs(key string, pageNum uint, pageSize uint) (res []model.EsProduct) {

	if pageNum == 0 {
		pageNum = 1
	}

	if pageSize == 0 {
		pageSize = 1
	}

	from := (pageNum - 1) * pageSize

	resp, err := EsClient.Search().
		Index(conf.Config.Es.ProductIndex).
		Query(&types.Query{
			Bool: &types.BoolQuery{
				Should: []types.Query{
					{MatchPhrase: map[string]types.MatchPhraseQuery{"name": {Query: key}}},
					{MatchPhrase: map[string]types.MatchPhraseQuery{"title": {Query: key}}},
					{MatchPhrase: map[string]types.MatchPhraseQuery{"info": {Query: key}}},
				},
			},
		}).
		From(int(from)).
		Size(int(pageSize)).
		Do(context.Background())
	if err != nil {
		fmt.Println("Failed to search in products", err)
		return
	}
	fmt.Printf("Result found, total: %d\n", resp.Hits.Total.Value)

	for _, hit := range resp.Hits.Hits {
		var esProduct model.EsProduct
		err = json.Unmarshal(hit.Source_, &esProduct)
		if err != nil {
			fmt.Println("decode failed")
		} else {
			res = append(res, esProduct)
			fmt.Println("decode success:", esProduct)
		}
	}
	return
}

func DeleteProductFromEs(id uint) error {
	resp, err := EsClient.Delete(conf.Config.Es.ProductIndex, strconv.FormatInt(int64(id), 10)).
		Do(context.Background())
	if err != nil {
		fmt.Println("delete document failed, err: ", err)
		return err
	}
	fmt.Println(resp.Result)
	return err
}

func UpdateProductFromEs(id uint, esProduct *model.EsProduct) error {
	fmt.Println("document id", id)
	resp, err := EsClient.Update(conf.Config.Es.ProductIndex, strconv.FormatInt(int64(id), 10)).
		Doc(esProduct).
		Do(context.Background())
	if err != nil {
		fmt.Println("update document err:", err)
		return err
	}
	fmt.Println(resp.Result)
	return err
}

func createProductIndex() {
	EsClient.Indices.Create(conf.Config.Es.ProductIndex).Do(context.Background())
}
