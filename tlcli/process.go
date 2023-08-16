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

func (this *Client) CreateTable(tableName string, columnsName map[string]COLUMNTYPE, indexs []string) (err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.Columns = make(map[string][]byte, 0)
	for k, v := range columnsName {
		tb.Columns[k] = []byte(v)
	}
	if indexs != nil {
		tb.Idx = make(map[string]int8, 0)
		for _, s := range indexs {
			if _, ok := tb.Columns[s]; ok {
				tb.Idx[s] = 0
			}
		}
	}
	if a, err := this.create(context.Background(), tb); err == nil {
		if !a.Ok {
			return errors.New(a.ErrorDesc)
		}
	} else {
		return err
	}
	return
}

func (this *Client) AlterTable(tableName string, columnsName map[string]COLUMNTYPE, indexs []string) (err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.Columns = make(map[string][]byte, 0)
	for k, v := range columnsName {
		tb.Columns[k] = []byte(v)
	}
	if indexs != nil {
		tb.Idx = make(map[string]int8, 0)
		for _, s := range indexs {
			if _, ok := tb.Columns[s]; ok {
				tb.Idx[s] = 0
			}
		}
	}
	if a, err := this.alter(context.Background(), tb); err == nil {
		if !a.Ok {
			return errors.New(a.ErrorDesc)
		}
	} else {
		return err
	}
	return
}

func (this *Client) Insert(tableName string, columnsMap map[string][]byte) (seq int64, _err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.Columns = columnsMap
	if ack, err := this.insert(context.Background(), tb); err == nil {
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

func (this *Client) SelectId(tableName string) (id int64, err error) {
	return this.selectId(context.Background(), tableName)
}

func (this *Client) SelectIdByIdx(tableName string, columnName string, columnValue []byte) (id int64, err error) {
	return this.selectIdByIdx(context.Background(), tableName, columnName, columnValue)
}

func (this *Client) SelectById(tableName string, id int64) (_db *DataBean, err error) {
	return this.selectById(context.Background(), tableName, id)
}

func (this *Client) SelectByIdx(tableName string, columnName string, columnValue []byte) (_db *DataBean, err error) {
	return this.selectByIdx(context.Background(), tableName, columnName, columnValue)
}

func (this *Client) SelectsByIdLimit(tableName string, startId, limit int64) (_db []*DataBean, err error) {
	return this.selectsByIdLimit(context.Background(), tableName, startId, limit)
}

func (this *Client) SelectAllByIdx(tableName string, columnName string, columnValue []byte) (_db []*DataBean, err error) {
	return this.selectAllByIdx(context.Background(), tableName, columnName, columnValue)
}

func (this *Client) SelectByIdxLimit(tableName string, columnName string, columnValue [][]byte, startId, limit int64) (_db []*DataBean, err error) {
	return this.selectByIdxLimit(context.Background(), tableName, columnName, columnValue, startId, limit)
}

func (this *Client) Update(tableName string, id int64, columnsMap map[string][]byte) (_err error) {
	tb := &TableBean{}
	tb.Name = tableName
	tb.ID = &id
	tb.Columns = columnsMap
	if ack, err := this.update(context.Background(), tb); err == nil {
		if !ack.Ack.Ok {
			_err = errors.New(ack.Ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}

func (this *Client) Delete(tableName string, id int64) (_err error) {
	tb := &TableBean{}
	tb.Name, tb.ID = tableName, &id
	if ack, err := this.delete(context.Background(), tb); err == nil {
		if !ack.Ack.Ok {
			_err = errors.New(ack.Ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}

func (this *Client) ShowAllTables() (_r []*TableBean, _err error) {
	return this.showAllTables(context.Background())
}

func (this *Client) ShowTable(tablename string) (_r *TableBean, _err error) {
	return this.showTable(context.Background(), tablename)
}

func (this *Client) Drop(tablename string) (_err error) {
	if ack, err := this.drop(context.Background(), tablename); err == nil {
		if !ack.Ok {
			_err = errors.New(ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}
