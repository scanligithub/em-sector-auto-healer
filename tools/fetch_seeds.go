package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

type StockMaster struct {
	Code     string `json:"code"`
	CodeName string `json:"code_name"`
}

func main() {
	fmt.Println("🚀 [Go TDX] 正在通过通达信协议获取全市场 A 股种子列表...")
	cli, err := tdx.DialDefault()
	if err != nil {
		fmt.Printf("❌ TDX 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	var masterList []StockMaster
	exchanges := []protocol.Exchange{protocol.ExchangeSH, protocol.ExchangeSZ, protocol.ExchangeBJ}

	for _, ex := range exchanges {
		resp, err := cli.GetCodeAll(ex)
		if err != nil || resp == nil {
			continue
		}
		for _, item := range resp.List {
			if ex == protocol.ExchangeSH {
				if strings.HasPrefix(item.Code, "60") || strings.HasPrefix(item.Code, "68") {
					masterList = append(masterList, StockMaster{Code: "SH" + item.Code, CodeName: item.Name})
				}
			} else if ex == protocol.ExchangeSZ {
				if strings.HasPrefix(item.Code, "00") || strings.HasPrefix(item.Code, "30") {
					masterList = append(masterList, StockMaster{Code: "SZ" + item.Code, CodeName: item.Name})
				}
			} else if ex == protocol.ExchangeBJ {
				if strings.HasPrefix(item.Code, "43") || strings.HasPrefix(item.Code, "83") ||
					strings.HasPrefix(item.Code, "87") || strings.HasPrefix(item.Code, "88") ||
					strings.HasPrefix(item.Code, "92") {
					masterList = append(masterList, StockMaster{Code: "BJ" + item.Code, CodeName: item.Name})
				}
			}
		}
	}

	file, err := os.Create("stock_list.json")
	if err != nil {
		fmt.Printf("❌ 写入文件失败: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	json.NewEncoder(file).Encode(masterList)
	fmt.Printf("✅ [Go TDX] 成功拉取 %d 只股票种子并保存至 stock_list.json\n", len(masterList))
}
