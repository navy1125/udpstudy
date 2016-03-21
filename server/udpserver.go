package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

var (
	recvHeadCh chan UdpHeader
	sendDataCh chan []byte
	HEADLEN    = 4
)

type UdpHeader struct {
	datasize  byte
	bitmask   byte
	seq       uint16
	ack       uint16
	timestamp int64
	data      []byte
}

func (self *UdpHeader) Serialize() []byte {
	self.datasize = byte(len(self.data))
	buf := bytes.NewBuffer(make([]byte, 0, 6+self.datasize))
	buf.WriteByte(self.datasize)
	buf.WriteByte(self.bitmask)
	binary.Write(buf, binary.LittleEndian, self.seq)
	binary.Write(buf, binary.LittleEndian, self.ack)
	buf.Write(self.data)
	return buf.Bytes()
}

func (self *UdpHeader) Unserialize(b []byte, all int) int {
	self.datasize = b[0]
	self.bitmask = b[1]
	if all < int(self.datasize)+6 {
		fmt.Println("Unserialize: ", all, int(self.datasize))
		return 0
	}
	seq := b[2:4]
	ack := b[4:6]
	self.seq = uint16((int(seq[1]) << 8) + int(seq[0]))
	self.ack = uint16((int(ack[1]) << 8) + int(ack[0]))
	if self.datasize > 0 {
		self.data = b[6 : 6+self.datasize]
	}
	fmt.Println(self.seq, self.ack, string(self.data))
	return 6 + int(self.datasize)
}

type UdpData struct {
	curseq uint16
	lastok uint16
	curack uint16
	header [65536]*UdpHeader
	wait   *bytes.Buffer
}

type Server struct {
	conn     *net.UDPConn
	recvData *UdpData
	sendData *UdpData
}

func (self *Server) CheckSendWaitData() bool {
	if self.sendData.wait == nil {
		return true
	}
	wait := self.sendData.wait
	self.sendData.wait = nil
	return self.sendData.SendData(wait.Bytes())
	return true
}

func (self *Server) SendData(b []byte) bool {
	bsize := len(b)
	for cur := 0; cur < bsize; {
		if self.sendData.curseq+1 == self.sendData.lastok {
			if self.sendData.wait == nil {
				self.sendData.wait = bytes.NewBuffer(nil)
			}
			self.sendData.wait.Write(b[cur:])
			return false
		}
		head := &UdpHeader{}
		head.seq = self.sendData.curseq
		head.timestamp = int64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())
		if bsize >= cur+256 {
			head.data = b[cur : cur+256]
			cur += 256
		} else {
			head.data = b[cur:bsize]
			cur = bsize
		}
		self.conn.Write(head.Serialize())
		self.sendData.curseq++
		if self.sendData.curseq == 65535 {
			self.sendData.curseq = 0
		}
	}
	return true
}

func (self *Server) Loop() {
	timersend := time.NewTimer(Millisecond)
	for {
		select {
		case head := <-recvHeadCh:
			if head.seq > self.recvData.lastok {
				self.recvData[head.seq] = head
				for i := self.recvData.lastok; i <= head.seq; i++ {
					if self.recvData.header[i] != nil {
						self.recvData.lastok++
					}
				}
			} else {
				fmt.Println("收到过期数据包")
			}
			if head.ack != 0 {
				self.sendData.header[head.ack] = nil
				for i := self.sendData.lastok; i <= head.ack; i++ {
					if self.sendData.header[i] == nil {
						self.sendData.lastok++
					}
				}
			}
		case data := <-sendDataCh:
			self.sendData.wait.Write(data)
		case <-timersend.C:
			self.CheckSendWaitData()
		}
	}
}

func main() {
	udpAddr, err := net.ResolveUDPAddr("udp", ":10001")
	s := &Server{
		recvData: &UdpData{},
		sendData: &UdpData{},
	}
	s.conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println("net.ListenUDP err:", err)
		return
	}
	b := make([]byte, 1024)
	left := 0
	for {
		n, addr, err := s.conn.ReadFromUDP(b[left:])
		if err != nil {
			fmt.Println("ERROR: ", err, n, addr)
			return
		}
		all := n + left
		for all >= 6 {
			head := &UdpHeader{}
			offset := head.Unserialize(b, all)
			if offset > 0 {
				//fmt.Println("ReadFromUDP: ", left, n, all, int(head.datasize))
				copy(b, b[offset:all])
				all -= offset
			} else {
				break
			}
			recvHeadCh <- head
		}
	}
}
