package udp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

var (
	HEADLEN = 4
)

type UdpHeader struct {
	datasize  byte
	bitmask   byte
	seq       uint16
	ack       uint16
	time_send int64
	time_ack  int64
	data      []byte
}

func (self *UdpHeader) Serialize() []byte {
	self.datasize = byte(len(self.data))
	buf := bytes.NewBuffer(make([]byte, 0, 6+int(self.datasize)))
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
		self.data = b[6 : 6+int(self.datasize)]
	}
	return 6 + int(self.datasize)
}

type UdpData struct {
	curseq uint16
	lastok uint16
	maxok  uint16
	curack uint16
	header [65536]*UdpHeader
}

type UdpTask struct {
	conn       *net.UDPConn
	addr       *net.UDPAddr
	recvData   *UdpData
	sendData   *UdpData
	wait       *bytes.Buffer
	recvHeadCh chan *UdpHeader
	waitHeader []*UdpHeader
	Test       bool
	is_server  bool
	num_resend int
	num_waste  int
}

func NewUdpTask() *UdpTask {
	task := &UdpTask{
		recvData:   &UdpData{},
		sendData:   &UdpData{},
		recvHeadCh: make(chan *UdpHeader, 1024),
	}
	return task
}

func (self *UdpTask) CheckSendWaitData() bool {
	for _, head := range self.waitHeader {
		n, _, err := self.conn.WriteMsgUDP(head.Serialize(), nil, self.addr)
		if n != int(head.datasize)+6 {
			fmt.Println("发送缓冲区有bug,无解", n, head.datasize+6, err)
			if err == nil {
				panic("ddddd")
			}
		}
		if n == 0 {
			return true
		}
		self.waitHeader = self.waitHeader[1:]
	}
	if self.wait == nil {
		return true
	}
	wait := self.wait
	self.wait = nil
	return self.SendData(wait.Bytes())
	return true
}

func (self *UdpTask) SendData(b []byte) bool {
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
		head.time_send = int64(time.Now().UnixNano() / int64(time.Millisecond))
		offset := 0
		if bsize >= cur+255 {
			head.data = b[cur : cur+255]
			offset = 255
		} else {
			head.data = b[cur:bsize]
			offset = bsize - cur
		}
		self.sendData.header[head.seq] = head
		self.conn.SetWriteDeadline(time.Now().Add(time.Duration(1 * 2 * int64(time.Second))))
		n, err := self.sendMsg(head)
		if err != nil {
			fmt.Println("消息发送失败", self.sendData.curseq, n, err)
		}
		if n == 0 {
			if self.wait == nil {
				self.wait = bytes.NewBuffer(nil)
			}
			self.wait.Write(b[cur:])
			return false
		} else {
			cur += offset
		}
		self.sendData.curseq++
		if self.sendData.curseq == 65535 {
			self.sendData.curseq = 0
		}
	}
	return true
}
func (self *UdpTask) sendMsg(head *UdpHeader) (int, error) {
	if self.sendData.maxok-self.sendData.lastok >= 100 && head.seq != self.sendData.lastok+1 {
		fmt.Println("缓冲区满,等待下次发送", head.seq, self.sendData.curseq, self.sendData.lastok, self.sendData.maxok)
		return 0, nil
	}
	n, _, err := self.conn.WriteMsgUDP(head.Serialize(), nil, self.addr)
	if n != int(head.datasize)+6 {
		fmt.Println("发送缓冲区有bug,无解", n, head.datasize+6, err)
		if err == nil {
			panic("ddddd")
		}
	}
	if n == 0 {
		if err == nil {
			panic("ddddd")
		}
		self.waitHeader = append(self.waitHeader, head)
	} else {
		head.time_send = int64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())
		//fmt.Println("消息发送", self.sendData.curseq, n, err)
	}
	return n, err
}

func (self *UdpTask) CheckSendLostMsg() {
	for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
		//fmt.Println("尝试丢包重发", i, self.sendData.lastok, self.sendData.maxok)
		if self.sendData.header[i] != nil && int64(time.Now().UnixNano()/int64(time.Millisecond))-self.sendData.header[i].time_send >= 10 {
			//发现有更新的包已经确认,所有老包直接重发
			n, _ := self.sendMsg(self.sendData.header[i])
			fmt.Println("丢包重发", i, n, self.sendData.lastok, self.sendData.maxok)
			if n == 0 {
				break
			}
			self.num_resend++
		}
	}
}
func (self *UdpTask) CheckSendAck() {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
		//fmt.Println("尝试丢包重发", i, self.sendData.lastok, self.sendData.maxok)
		if self.sendData.header[i] != nil {
			if self.sendData.header[i].time_ack != 0 {
				if now-self.sendData.header[i].time_ack < 10 {
					break
				} else {
					self.sendData.header[i] = nil
					continue
				}
			}
			if now-self.sendData.header[i].time_send >= 10 {
				//发现有更新的包已经确认,所有老包直接重发
				n, _ := self.sendMsg(self.sendData.header[i])
				fmt.Println("丢包重发", i, n, self.sendData.lastok, self.sendData.maxok)
				if n == 0 {
					break
				}
				self.sendData.header[i].time_send = now + 40
				self.num_resend++
			}
		}
	}
}
func (self *UdpTask) Loop() {
	timersend := time.NewTicker(time.Millisecond * 10)
	timerack := time.NewTicker(time.Millisecond * 50)
	timersec := time.NewTicker(time.Second)
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for {
		select {
		case head := <-self.recvHeadCh:
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			if head.datasize == 0 {
				fmt.Println("收包", head.seq)
				ismax := false
				if head.seq >= self.sendData.maxok {
					ismax = true
					self.sendData.maxok = head.seq
				}
				if head.bitmask&1 == 1 {
					fmt.Println("批量确认包完成", self.sendData.lastok, head.seq)
					for i := self.sendData.lastok; i <= head.seq; i++ {
						self.sendData.header[i] = nil
					}
				} else {
					//fmt.Println("确认包完成", head.seq)
					if self.sendData.header[head.seq] != nil {
						self.sendData.header[head.seq].time_ack = now
						//self.sendData.header[head.seq] = nil
					}
				}
				//这里尽量保证丢包后不要被多次重发,但是还是很难避免
				if ismax || self.sendData.maxok-self.sendData.lastok >= 100 {
					self.CheckSendLostMsg()
				}
				for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
					if self.sendData.header[i] != nil {
						//fmt.Println("等待乱序确认", self.sendData.lastok, self.sendData.maxok)
						break
					}
					self.sendData.lastok = i
				}
			} else if head.seq >= self.recvData.lastok && self.recvData.header[head.seq] == nil {
				self.recvData.header[head.seq] = head
				if head.seq > self.recvData.maxok {
					self.recvData.maxok = head.seq
				} else {
					head1 := &UdpHeader{}
					head1.seq = head.seq
					n, _ := self.sendMsg(head1)
					if n != 0 {
						head.seq = 0
					}
				}
				for i := self.recvData.lastok; i <= self.recvData.maxok; i++ {
					if self.recvData.header[i] == nil {
						break
					}
					self.recvData.lastok = i
				}
			} else {
				//收到过期数据,说明对方没有收到确认包,发一个
				head1 := &UdpHeader{}
				if head1.seq <= self.recvData.lastok {
					head1.bitmask |= 1
					head1.seq = self.recvData.lastok
				} else if head1.seq == self.recvData.lastok+1 {
					head1.bitmask |= 1
					head1.seq = self.recvData.lastok + 1
				} else {
					head1.seq = head.seq
				}
				if len(self.waitHeader) == 0 {
					self.sendMsg(head1)
				}
				self.num_waste++
				fmt.Println("收到过期数据包", head.seq, head.datasize, self.recvData.lastok, self.num_waste)
			}
		case <-timersec.C:
			fmt.Println("探测线程", self.sendData.lastok, self.sendData.maxok, self.num_resend, self.num_waste)
		case <-timerack.C:
			if len(self.waitHeader) == 0 {
				if self.recvData.curack < self.recvData.lastok {
					head := &UdpHeader{}
					head.seq = self.recvData.lastok
					head.bitmask |= 1
					n, _ := self.sendMsg(head)
					fmt.Println("批量确认:", self.recvData.curack, self.recvData.lastok)
					if n != 0 {
						self.recvData.curack = self.recvData.lastok
						self.recvData.header[self.recvData.curack].seq = 0
					}
					if self.recvData.lastok-self.recvData.curack >= 5 {
						self.sendMsg(head)
					}
				}
				for i := self.recvData.curack; i <= self.recvData.maxok; i++ {
					if self.recvData.header[i] != nil && self.recvData.header[i].seq != 0 {
						head := &UdpHeader{}
						head.seq = self.recvData.header[i].seq
						n, _ := self.sendMsg(head)
						if n != 0 {
							self.recvData.header[i].seq = 0
						}
					}
				}
			}
		case <-timersend.C:
			if self.sendData.maxok-self.sendData.lastok >= 100 {
				self.CheckSendLostMsg()
			}
			/*
				for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
					if self.sendData.header[i] != nil {
						//fmt.Println("检测超时", int64(time.Now().UnixNano()/int64(time.Millisecond))-self.sendData.header[i].time_send)
					}
					if self.sendData.header[i] != nil && int64(time.Now().UnixNano()/int64(time.Millisecond)) > self.sendData.header[i].time_send+2000 {
						//发现有更新的包已经确认,所有老包直接重发
						self.sendData.header[i].time_send = int64(time.Now().UnixNano() / int64(time.Millisecond))
						self.sendMsg(self.sendData.header[i].Serialize())
						fmt.Println("超时重发", i, self.sendData.lastok, self.sendData.maxok, self.sendData.header[i].time_send, int64(time.Now().UnixNano()/int64(time.Millisecond))-self.sendData.header[i].time_send+2000)
					}
				}
				// */
			if self.Test {
				if self.sendData.maxok < 65534 {
					self.SendData([]byte("wanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghai"))
				}
			}
			self.CheckSendWaitData()
		}
	}
}
func (self *UdpTask) LoopRecv() {
	b := make([]byte, 1024)
	left := 0
	for {
		n, addr, err := self.conn.ReadFromUDP(b[left:])
		if err != nil {
			fmt.Println("ERROR: ", err, n, addr)
			return
		}
		if self.is_server {
			self.addr = addr
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
			self.recvHeadCh <- head
		}
	}
}
func (self *UdpTask) Dial(addr *net.UDPAddr) (err error) {
	self.conn, err = net.DialUDP("udp", nil, addr)
	return err
}
func (self *UdpTask) Listen(addr *net.UDPAddr) (err error) {
	self.conn, err = net.ListenUDP("udp", addr)
	self.is_server = true
	return err
}
