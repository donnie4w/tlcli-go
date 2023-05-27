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
	"sync/atomic"
	"time"

	thrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/donnie4w/simplelog/logging"
)

var ConnectTimeout = 10 * time.Second
var SocketTimeout = 10 * time.Second
var _transportFactory = thrift.NewTBufferedTransportFactory(1 << 13)

type Client struct {
	Conn      *IcliClient
	transport thrift.TTransport
	hostPort  string
	tls       bool
	_auth     string
	_connid   int32
}

var TlClient = &Client{}

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
	if ack, err := this.Conn.Auth(context.Background(), this._auth); err == nil {
		if !ack.Ok {
			_err = errors.New(ack.ErrorDesc)
		}
	} else {
		_err = err
	}
	return
}

func (this *Client) ping() (_r *Ack, _err error) {
	_r, _err = this.Conn.Ping(context.Background(), 1)
	return
}

func timer(i int32) {
	ticker := time.NewTicker(3 * time.Second)
	for i == TlClient._connid {
		select {
		case <-ticker.C:
			func() {
				defer _recover()
				if ack, err := TlClient.ping(); err != nil || !ack.Ok {
					if i == TlClient._connid {
						TlClient.reLink()
					}
				}
			}()
		}
	}
}

func (this *Client) Close() (err error) {
	atomic.AddInt32(&TlClient._connid, 1)
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

func NewConnect(tls bool, hostPort string, auth string) (err error) {
	i := atomic.AddInt32(&TlClient._connid, 1)
	err = TlClient.Link(hostPort, auth, tls)
	go timer(i)
	return
}

func _recover() {
	if err := recover(); err != nil {
		logging.Error(err)
	}
}
