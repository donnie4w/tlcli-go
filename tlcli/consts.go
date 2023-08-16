// Copyright (c) , donnie <donnie4w@gmail.com>
// All rights reserved.
//
// github.com/donnie4w/tldb
// github.com/donnie4w/tlcli-go

package tlcli

type COLUMNTYPE string

const (
	STRING  COLUMNTYPE = "0"
	INT64   COLUMNTYPE = "1"
	INT32   COLUMNTYPE = "2"
	INT16   COLUMNTYPE = "3"
	INT8    COLUMNTYPE = "4"
	FLOAT64 COLUMNTYPE = "5"
	FLOAT32 COLUMNTYPE = "6"
	BINAY   COLUMNTYPE = "7"
	BYTE    COLUMNTYPE = "8"
	UINT64  COLUMNTYPE = "9"
	UINT32  COLUMNTYPE = "10"
	UINT16  COLUMNTYPE = "11"
	UINT8   COLUMNTYPE = "12"
)
