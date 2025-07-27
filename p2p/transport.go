package p2p

import "net"

// Peer is an interface that represents the remote node.
type Peer interface {
	net.Conn           // 表示一个网络连接，用于与其他节点通信
	Send([]byte) error // 发送数据
	CloseStream()      // 关闭流
}

// Transport is anything that handles the communication
// between the nodes in the network. This can be of the
// form (TCP, UDP, websockets, ...)
type Transport interface {
	Addr() string
	Dial(string) error
	ListenAndAccept() error
	Consume() <-chan RPC // 返回值是只读chan类型
	Close() error
}
