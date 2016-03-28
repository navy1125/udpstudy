package main

import (
	"../udp"
	"fmt"
	"net"
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", ":10001")
	udptask := udp.NewUdpTask()
	err = udptask.Listen(addr)
	if err != nil {
		fmt.Println("net.ListenUDP err:", err)
		return
	}
	go udptask.Loop()
	go udptask.LoopRecv()

	tcpaddr, err := net.ResolveTCPAddr("tcp", ":10002")
	if err != nil {
		fmt.Println("net.ResolveTCPAddr err:", err)
		return
	}
	tcptask := udp.NewTcpTask()
	err = tcptask.Listen(tcpaddr)
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	go tcptask.Loop()
	tcptask.LoopRecv()
}
