package main

import (
	"../udp"
	"fmt"
	"net"
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", ":10001")
	udp := udp.NewUdpTask()
	err = udp.Listen(addr)
	if err != nil {
		fmt.Println("net.ListenUDP err:", err)
		return
	}
	go udp.Loop()
	udp.LoopRecv()
}
