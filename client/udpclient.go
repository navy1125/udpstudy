package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
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
	buf := bytes.NewBuffer(b)
	self.datasize, _ = buf.ReadByte()
	self.bitmask, _ = buf.ReadByte()
	if all < int(self.datasize)+6 {
		fmt.Println("Unserialize: ", all, int(self.datasize))
		return 0
	}
	seq := make([]byte, 2)
	ack := make([]byte, 2)
	buf.Read(seq)
	buf.Read(ack)
	self.seq = uint16(int(seq[0])<<6 + int(seq[1]))
	self.ack = uint16(int(ack[0])<<6 + int(ack[1]))
	if self.datasize > 0 {
		var err error
		self.data, err = buf.ReadBytes(self.datasize)
		if err != nil {
			fmt.Println(err, self.datasize, buf.Len())
		}
	}
	fmt.Println(string(seq), string(ack), self.seq, self.ack, string(self.data))
	return 6 + int(self.datasize)
}

type UdpData struct {
	curseq uint16
	lastok uint16
	curack uint16
	header [65536]*UdpHeader
	wait   *bytes.Buffer
}

func (self *UdpData) SendData(b []byte) bool {
	bsize := len(b)
	for cur := 0; cur < bsize; {
		if self.curseq+1 == self.lastok {
			if self.wait == nil {
				self.wait = bytes.NewBuffer(nil)
			}
			self.wait.Write(b[cur:])
			return false
		}
		head := &UdpHeader{}
		head.seq = self.curseq
		head.timestamp = int64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())
		if bsize >= cur+256 {
			head.data = b[cur : cur+256]
			cur += 256
		} else {
			head.data = b[cur:bsize]
			cur = bsize
		}
		self.header[self.curseq] = head
		self.curseq++
		if self.curseq == 65535 {
			self.curseq = 0
		}
	}
	return true
}

type UdpClient struct {
	conn     *net.UDPConn
	recvData *UdpData
	sendData *UdpData
}

func main() {
	addr, err := net.ResolveUDPAddr("udp", ":10001")
	if err != nil {
		fmt.Println("net.ResolveUDPAddr err:", err)
		return
	}
	c := &UdpClient{
		recvData: &UdpData{},
		sendData: &UdpData{},
	}
	c.conn, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("net.DialUDP err:", err)
		return
	}
	for i := 0; i < 1000; i++ {
		c.sendData.SendData([]byte("wanghaijun"))
		for j := c.sendData.lastok; j <= c.sendData.curseq; j++ {
			head := c.sendData.header[j]
			if head != nil {
				fmt.Println("sendData: ", i, head.seq, c.sendData.curseq, len(head.Serialize()))
				n, err := c.conn.Write(head.Serialize())
				if err != nil {
					fmt.Println("ERROR: ", err, n)
					return
				}
			}
		}
	}
}
