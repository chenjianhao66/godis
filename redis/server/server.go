package server

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"fmt"
	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.DB
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a Handler instance
//
// 判断配置文件对象的Self字段是否存在，存在这是集群模式启动，否则就是单节点启动
func MakeHandler() *Handler {
	var db database.DB
	// 查看服务的配置文件是否有self字段，有的话代表着是集群模式启动
	if config.Properties.Self != "" &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeCluster()
	} else {
		// 否则则是单节点启动
		db = database2.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	// 如果当前的Handler对象已经close的话，则直接关闭掉该连接并退出
	// 那什么情况会出现closing字段会被设置呢？
	// TODO 需要找出closing字段被设置的代码
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	// 传入net包的连接对象，返回connection包的连接对象
	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	// 根据conn连接对象获取一个只读消息的通道，该通道会返回 Payload 类型的数据
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		fmt.Printf("从管道中读取到数据 --> \n%s \n", payload.Data.ToBytes())
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)

		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		// 解析协议，并把命令赋值给Args，最后执行Args的命令
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
