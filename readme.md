### client for tldb in go

------------

See the example at  https://tlnet.top/tlcli

```go
实例化，新建连接
client, err := tlcli.NewConnect(false, "127.0.0.1:7001", "mycli=123")
1.NewConnect第一个参数是 是否 使用tls。如果服务器启动客户端安全链接协议，那么客户端应该将该参数设置为true。
2.NewConnect第二个参数 是服务器启动客户端服务ip与端口
3.NewConnect第三个参数 是访问的用户名密码，用等于号连接起来

新建表
client.CreateTable("user", map[string]COLUMNTYPE{"name": STRING, "age": INT8, "level": INT16d}, []string{"name", "level"})
1.CreateTable第一个参数为 表名
2.CreateTable第二个参数为 字段名:字段类型
3.CreateTable第三个参数为 索引名，即需要建立索引的字段名，字符串数组类型

修改表结构
client.AlterTable("user", map[string]COLUMNTYPE{"name": STRING, "age": INT8, "level": INT16, "type": INT16,}, []string{"name", "level"})
1.AlterTable第一个参数为 表名
2.AlterTable第二个参数为 字段名:字段类型
3.AlterTable第三个参数为 索引名，即需要建立索引的字段名，字符串数组类型

删除表及表数据
client.Drop("user")
1.Drop 参数为 表名

获取指定表结构的信息
1.client.ShowTable("user")

获取数据库所有表的信息
1.client.ShowAllTables()

新增数据
ms := make(map[string][]byte, 0)
ms["name"], ms["age"] =[]byte("tom"),int8Tobytes(1)
client.Insert("user", ms)
1.Insert第一个参数为 表名
2.Insert第二个参数为Map类型  key为字段名，value为字段值
字段名为字符串类型，字段值为字节数组
如果值为数字或其他类型，需要做类型转换，数据库以字节数组存储。

更新数据
ms := make(map[string][]byte, 0)
ms["name"], ms["age"] =[]byte("tom"),int8Tobytes(2)
client.Update("user", ms)
1.Update第一个参数为 表名
2.Update第二个参数为Map类型  key为字段名，value为字段值
字段名为字符串类型，字段值为字节数组
如果值为数字或其他类型，需要做类型转换，数据库以字节数组存储。

删除数据
client.Delete("user",1)
1.Delete第一个参数为 表名
2.Delete第二个参数为 表id值
```
