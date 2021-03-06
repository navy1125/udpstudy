package main

import (
	"../udp"
	"fmt"
	"net"
)

var (
	url = "14.17.104.56"
	//url = "127.0.0.1"
)

func main() {
	addr_udp, err := net.ResolveUDPAddr("udp", url+":10001")
	addr_tcp, err := net.ResolveTCPAddr("tcp", url+":10002")
	if err != nil {
		fmt.Println("net.ResolveUDPAddr err:", err)
		return
	}
	udptask := udp.NewUdpTask()
	err = udptask.Dial(addr_udp, addr_tcp)
	if err != nil {
		fmt.Println("net.DialUDP err:", err)
		return
	}
	udptask.Test = true
	go udptask.Loop()
	go udptask.LoopRecvUDP()
	udptask.LoopRecvTCP()
	return
	/*

		tcpaddr, err := net.ResolveTCPAddr("tcp", url+":11001")
		if err != nil {
			fmt.Println("net.ResolveTCPAddr err:", err)
			return
		}
		tcptask := udp.NewTcpTask()
		err = tcptask.Dial(tcpaddr)
		if err != nil {
			fmt.Println("net.DialUDP err:", err)
			return
		}
		//tcptask.Test = true
		go tcptask.Loop()
		tcptask.LoopRecv()
		// */
}
