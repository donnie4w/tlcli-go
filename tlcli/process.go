// Copyright (c) , donnie <donnie4w@gmail.com>
// All rights reserved.
//
// github.com/donnie4w/tldb
// github.com/donnie4w/tlcli-go

package tlcli

import (
	"context"
	"errors"
)

func CreateTable(tableName string, columnsName []string, indexs []string) (err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.Columns = make(map[string][]byte, 0)
	for _, s := range columnsName {
		tb.Columns[s] = nil
	}
	if indexs != nil {
		tb.Idx = make(map[string]int8, 0)
		for _, s := range indexs {
			if _, ok := tb.Columns[s]; ok {
				tb.Idx[s] = 0
			}
		}
	}
	if a, err := TlClient.Conn.Create(context.Background(), tb); err == nil {
		if !a.Ok {
			return errors.New(a.ErrorDesc)
		}
	} else {
		return err
	}
	return
}

func AlterTable(tableName string, columnsName []string, indexs []string) (err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.Columns = make(map[string][]byte, 0)
	for _, s := range columnsName {
		tb.Columns[s] = nil
	}
	if indexs != nil {
		tb.Idx = make(map[string]int8, 0)
		for _, s := range indexs {
			if _, ok := tb.Columns[s]; ok {
				tb.Idx[s] = 0
			}
		}
	}
	if a, err := TlClient.Conn.Alter(context.Background(), tb); err == nil {
		if !a.Ok {
			return errors.New(a.ErrorDesc)
		}
	} else {
		return err
	}
	return
}

func Insert(tableName string, columnsMap map[string][]byte) (seq int64, _err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.Columns = columnsMap
	if ack, err := TlClient.Conn.Insert(context.Background(), tb); err == nil {
		if ack.Ack.Ok {
			seq = ack.Seq
		} else {
			_err = errors.New(ack.Ack.GetErrorDesc())
		}
	} else {
		_err = err
	}
	return
}

func SelectById(tableName string, id int64) (_db *DataBean, err error) {
	return TlClient.Conn.SelectById(context.Background(), tableName, id)
}

func SelectByIdx(tableName string, columnName string, columnValue []byte) (_db *DataBean, err error) {
	return TlClient.Conn.SelectByIdx(context.Background(), tableName, columnName, columnValue)
}

func SelectsByIdLimit(tableName string, startId, limit int64) (_db []*DataBean, err error) {
	return TlClient.Conn.SelectsByIdLimit(context.Background(), tableName, startId, limit)
}

func SelectAllByIdx(tableName string, columnName string, columnValue []byte) (_db []*DataBean, err error) {
	return TlClient.Conn.SelectAllByIdx(context.Background(), tableName, columnName, columnValue)
}

func SelectByIdxLimit(tableName string, columnName string, columnValue [][]byte, startId, limit int64) (_db []*DataBean, err error) {
	return TlClient.Conn.SelectByIdxLimit(context.Background(), tableName, columnName, columnValue, startId, limit)
}

func Update(tableName string, id int64, columnsMap map[string][]byte) (_err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.ID = &id
	tb.Columns = columnsMap
	if ack, err := TlClient.Conn.Update(context.Background(), tb); err == nil {
		if !ack.Ack.Ok {
			_err = errors.New(ack.Ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}

func Delete(tableName string, id int64) (_err error) {
	tb := &TableBean{}
	tb.Name, tb.ID = tableName, &id
	if ack, err := TlClient.Conn.Delete(context.Background(), tb); err == nil {
		if !ack.Ack.Ok {
			_err = errors.New(ack.Ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}

func ShowAllTables() (_r []*TableBean, _err error) {
	return TlClient.Conn.ShowAllTables(context.Background())
}

func ShowTable(tablename string) (_r *TableBean, _err error) {
	return TlClient.Conn.ShowTable(context.Background(), tablename)
}

func Truncate(tablename string) (_err error) {
	if ack, err := TlClient.Conn.Truncate(context.Background(), tablename); err == nil {
		if !ack.Ok {
			_err = errors.New(ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}
