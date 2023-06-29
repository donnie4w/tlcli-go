// Copyright (c) , donnie <donnie4w@gmail.com>
// All rights reserved.
//
// github.com/donnie4w/tldb
// github.com/donnie4w/tlcli-go

package tlcli

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	thrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/donnie4w/simplelog/logging"
)

var ConnectTimeout = 60 * time.Second
var SocketTimeout = 60 * time.Second
var _transportFactory = thrift.NewTBufferedTransportFactory(1 << 13)

type Client struct {
	Conn      *IcliClient
	transport thrift.TTransport
	hostPort  string
	tls       bool
	_auth     string
	_connid   int32
	mux       *sync.Mutex
	_pingNum  int32
}

func (this *Client) Link(hostPort, auth string, TLS bool) (err error) {
	defer _recover()
	this.tls, this.hostPort, this._auth = TLS, hostPort, auth
	tcf := &thrift.TConfiguration{ConnectTimeout: ConnectTimeout, SocketTimeout: SocketTimeout}
	var transport thrift.TTransport
	if this.tls {
		tcf.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		transport = thrift.NewTSSLSocketConf(hostPort, tcf)
	} else {
		transport = thrift.NewTSocketConf(hostPort, tcf)
	}
	var useTransport thrift.TTransport
	if err == nil && transport != nil {
		if useTransport, err = _transportFactory.GetTransport(transport); err == nil {
			if err = useTransport.Open(); err == nil {
				this.hostPort = hostPort
				this.transport = useTransport
				this.Conn = NewIcliClientFactory(useTransport, thrift.NewTCompactProtocolFactory())
				<-time.After(1 * time.Second)
				this._pingNum = 0
				err = this.auth()
			}
		}
	}
	if err != nil {
		logging.Error("client to [", hostPort, "] Error:", err)
	}
	return
}

func (this *Client) auth() (_err error) {
	if ack, err := this.auth0(context.Background(), this._auth); err == nil {
		if !ack.Ok {
			_err = errors.New(ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}

func (this *Client) timer(i int32) {
	ticker := time.NewTicker(3 * time.Second)
	for i == this._connid {
		select {
		case <-ticker.C:
			func() {
				defer _recover()
				if this._pingNum > 5 && i == this._connid {
					this.reLink()
					return
				}
				atomic.AddInt32(&this._pingNum, 1)
				if ack, err := this.ping(context.Background(), 1); err == nil && ack.Ok {
					atomic.AddInt32(&this._pingNum, -1)
				}
			}()
		}
	}
}

func (this *Client) Close() (err error) {
	atomic.AddInt32(&this._connid, 1)
	if this.transport != nil {
		err = this.transport.Close()
	}
	return
}

func (this *Client) reLink() error {
	logging.Warn("reconnect")
	if this.transport != nil {
		this.transport.Close()
	}
	return this.Link(this.hostPort, this._auth, this.tls)
}

func NewConnect(tls bool, hostPort string, auth string) (client *Client, err error) {
	client = &Client{mux: &sync.Mutex{}}
	i := atomic.AddInt32(&client._connid, 1)
	err = client.Link(hostPort, auth, tls)
	go client.timer(i)
	return
}

// Parameters:
//   - I
func (this *Client) ping(ctx context.Context, i int64) (_r *Ack, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Ping(ctx, i)
}

// Parameters:
//   - S
func (this *Client) auth0(ctx context.Context, s string) (_r *Ack, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Auth(ctx, s)
}

// Parameters:
//   - Tb
func (this *Client) create(ctx context.Context, tb *TableBean) (_r *Ack, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Create(ctx, tb)
}

// Parameters:
//   - Tb
func (this *Client) alter(ctx context.Context, tb *TableBean) (_r *Ack, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Alter(ctx, tb)
}

// Parameters:
//   - Name
func (this *Client) truncate(ctx context.Context, name string) (_r *Ack, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Drop(ctx, name)
}

// Parameters:
//   - Name
//   - ID
func (this *Client) selectById(ctx context.Context, name string, id int64) (_r *DataBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.SelectById(ctx, name, id)
}

// Parameters:
//   - Name
//   - Column
//   - Value
func (this *Client) selectByIdx(ctx context.Context, name string, column string, value []byte) (_r *DataBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.SelectByIdx(ctx, name, column, value)
}

// Parameters:
//   - Name
//   - StartId
//   - Limit
func (this *Client) selectsByIdLimit(ctx context.Context, name string, startId int64, limit int64) (_r []*DataBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.SelectsByIdLimit(ctx, name, startId, limit)
}

// Parameters:
//   - Name
//   - Column
//   - Value
func (this *Client) selectAllByIdx(ctx context.Context, name string, column string, value []byte) (_r []*DataBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.SelectAllByIdx(ctx, name, column, value)
}

// Parameters:
//   - Name
//   - Column
//   - Value
//   - StartId
//   - Limit
func (this *Client) selectByIdxLimit(ctx context.Context, name string, column string, value [][]byte, startId int64, limit int64) (_r []*DataBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.SelectByIdxLimit(ctx, name, column, value, startId, limit)
}

// Parameters:
//   - Tb
func (this *Client) update(ctx context.Context, tb *TableBean) (_r *AckBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Update(ctx, tb)
}

// Parameters:
//   - Tb
func (this *Client) delete(ctx context.Context, tb *TableBean) (_r *AckBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Delete(ctx, tb)
}

// Parameters:
//   - Tb
func (this *Client) insert(ctx context.Context, tb *TableBean) (_r *AckBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.Insert(ctx, tb)
}

// Parameters:
//   - Name
func (this *Client) showTable(ctx context.Context, name string) (_r *TableBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.ShowTable(ctx, name)
}
func (this *Client) showAllTables(ctx context.Context) (_r []*TableBean, _err error) {
	defer _recover()
	defer this.mux.Unlock()
	this.mux.Lock()
	return this.Conn.ShowAllTables(ctx)
}

func _recover() {
	if err := recover(); err != nil {
		logging.Error(err)
	}
}
