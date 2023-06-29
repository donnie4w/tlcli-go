// Copyright (c) , donnie <donnie4w@gmail.com>
// All rights reserved.
//
// github.com/donnie4w/tldb
// github.com/donnie4w/tlcli-go
package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/donnie4w/simplelog/logging"
	"github.com/donnie4w/tlcli-go/tlcli"
)

func Test_cli(t *testing.T) {
	if client, err := tlcli.NewConnect(true, "192.168.2.108:7000", "mycli=123"); err == nil {
		err = client.Drop("pyuser")
		logging.Error(err)
		client.CreateTable("user", []string{"name", "age", "level", "type"}, []string{"name", "age", "type"})
		dbs, _ := client.ShowAllTables()
		logging.Debug(dbs)
		logging.Debug("-------------------------------------------------------------------")
		for k := int64(0); k < 8; k++ {
			go func() {
				for i := int64(0); i < 1; i++ {
					ms := make(map[string][]byte, 0)
					ms["name"], ms["age"], ms["level"], ms["type"] = []byte("wuxiaodong"), []byte(fmt.Sprint(20+i)), []byte(fmt.Sprint("level", i)), []byte(fmt.Sprint(i%2))
					if seq, err := client.Insert("user", ms); err == nil {
						logging.Debug("seq=====>", seq)
						_db, _ := client.SelectById("user", seq)
						logging.Debug(*_db)
					} else {
						logging.Error(err)
					}
				}
			}()
		}
		// tlcli.Delete("table", 2)
		logging.Debug("-------------------------------------------------------------------")
		// err := tlcli.Update("table", 45, map[string][]byte{"name": []byte("dong")})
		// logging.Error(err)
		// _db, _ := tlcli.SelectById("table", 2)
		// logging.Debug(*_db)
		logging.Debug("-------------------------------------------------------------------")
		// _db, _ := tlcli.SelectByIdx("table", "name", []byte("dongdongdong"))
		// logging.Debug(*_db)
		// logging.Debug("-------------------------------------------------------------------")
		// _dbs, _ := tlcli.SelectAllByIdx("table", "name", []byte("wuxiaodong"))
		// for _, db := range _dbs {
		// 	logging.Debug(*db)
		// }
		// logging.Debug("-------------------------------------------------------------------")
		_dbs, _ := client.SelectByIdxLimit("user", "name", [][]byte{[]byte("wuxiaodong"), []byte("dong")}, 0, 20)
		for _, db := range _dbs {
			logging.Debug(*db)
		}
		logging.Debug("-------------------------------------------------------------------")
	} else {
		logging.Error(err)
	}
	time.Sleep(1000 * time.Second)
}
