package main

import (
	"../udp"
	"fmt"
	"net"
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", "14.17.104.56:10001")
	//addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	if err != nil {
		fmt.Println("net.ResolveUDPAddr err:", err)
		return
	}
	udp := udp.NewUdpTask()
	err = udp.Dial(addr)
	if err != nil {
		fmt.Println("net.DialUDP err:", err)
		return
	}
	udp.Test = true
	go udp.Loop()
	udp.LoopRecv()
}
