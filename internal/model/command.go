package model

import "net"

type Command struct {
	Type         int      `json:"type"`
	Topic        string   `json:"topic"`
	Body         string   `json:"body"`
	ConsumerName string   `json:"consumerName"`
	Offset       uint     `json:"offset"`
	Connection   net.Conn `json:"connection"`
}

type Response struct {
	Body   string `json:"body"`
	Type   int    `json:"type"`
	Offset uint   `json:"offset"`
}
