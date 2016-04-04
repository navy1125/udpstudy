package main

import (
	"../udp"
	"fmt"
	"net"
)

func main() {
	addr_udp, err := net.ResolveUDPAddr("udp", ":10001")
	addr_tcp, err := net.ResolveTCPAddr("tcp", ":10002")
	udptask := udp.NewUdpTask()
	err = udptask.Listen(addr_udp, addr_tcp)
	if err != nil {
		fmt.Println("net.ListenUDP err:", err)
		return
	}
	go udptask.Loop()
	go udptask.LoopRecvUDP()
	udptask.LoopRecvTCP()
	return

	/*
		tcpaddr, err := net.ResolveTCPAddr("tcp", ":11001")
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
		// */
}
