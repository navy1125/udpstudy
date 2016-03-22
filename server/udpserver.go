package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

var (
	recvHeadCh = make(chan *UdpHeader, 1024)
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
	maxok  uint16
	curack uint16
	header [65536]*UdpHeader
}

type Server struct {
	conn     *net.UDPConn
	addr     *net.UDPAddr
	recvData *UdpData
	sendData *UdpData
	wait     *bytes.Buffer
}

func (self *Server) CheckSendWaitData() bool {
	if self.wait == nil {
		return true
	}
	wait := self.wait
	self.wait = nil
	return self.SendData(wait.Bytes())
	return true
}

func (self *Server) SendData(b []byte) bool {
	bsize := len(b)
	for cur := 0; cur < bsize; {
		if self.sendData.curseq+1 == self.sendData.lastok {
			if self.wait == nil {
				self.wait = bytes.NewBuffer(nil)
			}
			self.wait.Write(b[cur:])
			return false
		}
		head := &UdpHeader{}
		head.seq = self.sendData.curseq
		head.timestamp = int64(time.Now().UnixNano() / int64(time.Millisecond))
		if bsize >= cur+256 {
			head.data = b[cur : cur+256]
			cur += 256
		} else {
			head.data = b[cur:bsize]
			cur = bsize
		}
		self.conn.WriteToUDP(head.Serialize(), self.addr)
		self.sendData.curseq++
		if self.sendData.curseq == 65535 {
			self.sendData.curseq = 0
		}
	}
	return true
}

func (self *Server) Loop() {
	timersend := time.NewTicker(time.Millisecond)
	for {
		select {
		case head := <-recvHeadCh:
			if head.datasize == 0 {
				ismax := false
				if head.seq >= self.sendData.maxok {
					ismax = true
					self.sendData.maxok = head.seq
				}
				if head.bitmask&1 == 1 {
					for i := self.sendData.lastok; i <= head.seq; i++ {
						fmt.Println("确认包完成", i)
						self.sendData.header[i] = nil
					}
				} else {
					fmt.Println("确认包完成", head.seq)
					self.sendData.header[head.seq] = nil
				}
				//这里尽量保证丢包后不要被多次重发,但是还是很难避免
				if ismax {
					for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
						if self.sendData.header[i] != nil {
							//发现有更新的包已经确认,所有老包直接重发
							//self.sendData.header[i] = int64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())
							self.conn.Write(self.sendData.header[i].Serialize())
							fmt.Println("丢包重发")
						}
					}
				}
				fmt.Println("检测丢包", self.sendData.lastok, self.sendData.maxok)
				for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
					if self.sendData.header[i] != nil {
						break
					}
					self.sendData.lastok = i
				}
			} else if head.seq >= self.recvData.lastok && self.recvData.header[head.seq] == nil {
				self.recvData.header[head.seq] = head
				if head.seq > self.recvData.maxok {
					self.recvData.maxok = head.seq
				}
				for i := self.recvData.lastok; i <= self.recvData.maxok; i++ {
					if self.recvData.header[i] == nil {
						break
					}
					self.recvData.lastok = i
				}
			} else {
				//收到过期数据,说明对方没有收到确认包,发一个
				head.datasize = 0
				self.conn.Write(head.Serialize())
				fmt.Println("收到过期数据包", head.seq, self.recvData.lastok)
			}
		case <-timersend.C:
			if self.recvData.curack < self.recvData.lastok {
				fmt.Println("wwww", self.recvData.curack, self.recvData.lastok, self.recvData.maxok)
				head := &UdpHeader{}
				head.seq = self.recvData.lastok
				head.bitmask |= 1
				self.conn.Write(head.Serialize())
				self.recvData.curack = self.recvData.lastok
				self.recvData.header[self.recvData.curack].seq = 0
			}
			for i := self.recvData.curack; i <= self.recvData.maxok; i++ {
				if self.recvData.header[i] != nil && self.recvData.header[i].seq != 0 {
					head := &UdpHeader{}
					head.seq = self.recvData.header[i].seq
					self.conn.Write(head.Serialize())
					self.recvData.header[i].seq = 0
				}
			}
			for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
				if self.sendData.header[i] != nil {
					fmt.Println("检测超时", int64(time.Now().UnixNano()/int64(time.Millisecond))-self.sendData.header[i].timestamp)
				}
				if self.sendData.header[i] != nil && int64(time.Now().UnixNano()/int64(time.Millisecond)) > self.sendData.header[i].timestamp+2000 {
					//发现有更新的包已经确认,所有老包直接重发
					self.sendData.header[i].timestamp = int64(time.Now().UnixNano() / int64(time.Millisecond))
					self.conn.Write(self.sendData.header[i].Serialize())
					fmt.Println("超时重发")
				}
			}
			if self.sendData.curseq < 1024 {
				self.SendData([]byte("wanghaijun"))
			}
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
	go s.Loop()
	b := make([]byte, 1024)
	left := 0
	for {
		n, addr, err := s.conn.ReadFromUDP(b[left:])
		if err != nil {
			fmt.Println("ERROR: ", err, n, addr)
			return
		}
		s.addr = addr
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
