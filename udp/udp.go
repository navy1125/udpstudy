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
	datasize     byte
	bitmask      byte
	seq          uint16
	ack          uint16
	time_send    int64
	time_ack     int64
	resend_times int64
	data         []byte
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
	conn             *net.UDPConn
	addr             *net.UDPAddr
	recvData         *UdpData
	sendData         *UdpData
	wait             *bytes.Buffer
	recvDataCh       chan *UdpHeader
	recvAckCh        chan *UdpHeader
	last_ack         *UdpHeader
	Test             bool
	is_server        bool
	num_resend       int
	num_waste        int
	num_recv_data    int
	num_recv_ack     int
	num_recv_acklist int
	num_send         int
	num_ack          int
	num_acklist      int
	num_timeout      int
	ping             int64
}

func NewUdpTask() *UdpTask {
	task := &UdpTask{
		recvData:   &UdpData{},
		sendData:   &UdpData{},
		recvDataCh: make(chan *UdpHeader, 1024),
		recvAckCh:  make(chan *UdpHeader, 1024),
		ping:       10,
	}
	return task
}

func (self *UdpTask) CheckSendWaitData() bool {
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
	if head.seq-self.sendData.lastok >= 100 && head.seq != self.sendData.lastok+1 {
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
	} else {
		head.time_send = int64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())
		//fmt.Println("消息发送", self.sendData.curseq, n, err)
	}
	self.num_send++
	return n, err
}
func (self *UdpTask) sendAck(head *UdpHeader) (int, error) {
	n, _, err := self.conn.WriteMsgUDP(head.Serialize(), nil, self.addr)
	if n != int(head.datasize)+6 {
		fmt.Println("发送缓冲区有bug,无解", n, head.datasize+6, err)
		if err == nil {
			panic("ddddd")
		}
	}
	return n, err
}
func (self *UdpTask) CheckReSendAck() {
	if self.last_ack != nil {
		n, _, _ := self.conn.WriteMsgUDP(self.last_ack.Serialize(), nil, self.addr)
		if n != 0 {
			self.last_ack = nil
			self.num_acklist++
		}
	}
}
func (self *UdpTask) CheckSendLostMsg() {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	resendmax := uint16(0)
	for resendmax = self.sendData.lastok; resendmax <= self.sendData.maxok; resendmax++ {
		if self.sendData.header[resendmax] != nil {
			if self.sendData.header[resendmax].time_ack != 0 {
				if now-self.sendData.header[resendmax].time_ack < self.ping {
					//fmt.Println("等待单个确认包完成", resendmax, self.sendData.lastok, now, self.sendData.header[resendmax].time_ack, now-self.sendData.header[resendmax].time_ack)
					break
				} else {
					//fmt.Println("单个确认包完成", resendmax, self.sendData.lastok, now, self.sendData.header[resendmax].time_ack, now-self.sendData.header[resendmax].time_ack)
					self.sendData.header[resendmax] = nil
					self.CheckLastok()
					continue
				}
			}
		}
	}
	for i := self.sendData.lastok; i < uint16(resendmax); i++ {
		//fmt.Println("尝试丢包重发", i, self.sendData.lastok, self.sendData.maxok)
		if self.sendData.header[i] != nil {
			if now-self.sendData.header[i].time_send >= self.ping {
				//发现有更新的包已经确认,所有老包直接重发
				oldtime := self.sendData.header[i].time_send
				n, _ := self.sendMsg(self.sendData.header[i])
				fmt.Println("丢包重发", i, n, self.sendData.lastok, self.sendData.maxok, now, oldtime, now-oldtime)
				if n == 0 {
					break
				}
				self.sendData.header[i].time_send = now + self.sendData.header[i].resend_times*50 + 40
				self.sendData.header[i].resend_times++
				self.num_resend++
			}
		}
	}
}
func (self *UdpTask) CheckLastok() {
	for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
		//if self.sendData.header[i] != nil && self.sendData.header[i].time_ack != 0 {
		if self.sendData.header[i] != nil {
			if self.sendData.maxok-self.sendData.lastok >= 100 {
				fmt.Println("等待乱序确认异常", self.sendData.lastok, self.sendData.maxok)
			}
			break
		}
		self.sendData.lastok = i
	}
}
func isset_state(state byte, teststate uint16) bool {
	//return 0 != (state[teststate/8] & (0xff & (1 << (teststate % 8))))
	return 0 != (state & (0xff & (1 << (teststate % 8))))
}

func set_state(state byte, teststate uint16) byte {
	state |= (0xff & (1 << (teststate % 8)))
	return state
}
func (self *UdpTask) FillOkAckHead(head *UdpHeader) {
	self.FillMaxokAckHead(head)
	head.datasize = 0
	for i := uint16(1); i <= 8; i++ {
		next := head.seq + i
		if next < self.recvData.maxok {
			if self.recvData.header[next] != nil {
				head.datasize = set_state(head.datasize, i)
			}
		} else {
			break
		}
	}
}
func (self *UdpTask) FillMaxokAckHead(head *UdpHeader) {
	head.bitmask = 0
	for i := uint16(8); i > 2; i-- {
		next := head.seq - i
		if self.recvData.header[next] != nil {
			head.bitmask = set_state(head.bitmask, i)
		}
	}
	if head.bitmask != 0 {
		fmt.Println("FillMaxokAckHead", head.bitmask)
	}
}
func (self *UdpTask) FillLastokAckHead(head *UdpHeader) {
	head.datasize = 0
	for i := uint16(1); i <= 8; i++ {
		next := head.seq + i + 1
		if next < self.recvData.maxok {
			if self.recvData.header[next] != nil {
				head.datasize = set_state(head.datasize, i)
			}
		} else {
			break
		}
	}
}
func (self *UdpTask) Loop() {
	timersend := time.NewTicker(time.Millisecond * 10)
	timerack := time.NewTicker(time.Millisecond * 50)
	timersec := time.NewTicker(time.Second)
	timercheckack := time.NewTimer(time.Millisecond * 10)
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for {
		select {
		case head := <-self.recvAckCh:
			//self.Test = true
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			//fmt.Println("收包", head.seq)
			ismax := false
			if head.seq >= self.sendData.maxok {
				ismax = true
				self.sendData.maxok = head.seq
				if self.sendData.header[head.seq] != nil {
					self.ping = now - self.sendData.header[head.seq].time_send
				}
			}
			if head.bitmask&1 == 1 {
				self.num_recv_acklist++
				//fmt.Println("批量确认包完成", self.sendData.lastok, head.seq)
				for i := self.sendData.lastok; i <= head.seq; i++ {
					self.sendData.header[i] = nil
				}
				if head.datasize != 0 {
					for i := uint16(1); i <= 8; i++ {
						if isset_state(head.datasize, i) {
							next := head.seq + i + 1
							if self.sendData.header[next] != nil && self.sendData.header[next].time_ack == 0 {
								fmt.Println("last datasize补漏成功:", head.seq, head.bitmask)
								self.sendData.header[head.seq].time_ack = now
							}
						}
					}
				}
			} else {
				self.num_recv_ack++
				//fmt.Println("收到单个确认包", head.seq)
				if self.sendData.header[head.seq] != nil {
					self.sendData.header[head.seq].time_ack = now
					//self.sendData.header[head.seq] = nil
				}
				if head.datasize != 0 {
					for i := uint16(1); i <= 8; i++ {
						if isset_state(head.datasize, i) {
							next := head.seq + i
							if self.sendData.header[next] != nil && self.sendData.header[next].time_ack == 0 {
								fmt.Println("datasize补漏成功:", head.seq, head.bitmask)
								self.sendData.header[head.seq].time_ack = now
							}
						}
					}
				}
				if head.bitmask != 0 {
					for i := uint16(8); i >= 2; i-- {
						if isset_state(head.bitmask, i) {
							next := head.seq + i
							if self.sendData.header[next] != nil && self.sendData.header[next].time_ack == 0 {
								fmt.Println("bitmask补漏成功:", head.seq, head.bitmask)
								self.sendData.header[head.seq].time_ack = now
							}
						}
					}
				}
			}
			//这里尽量保证丢包后不要被多次重发,但是还是很难避免
			if ismax || self.sendData.maxok-self.sendData.lastok >= 100 {
				self.CheckSendLostMsg()
			}
			self.CheckLastok()
		case head := <-self.recvDataCh:
			if head.seq >= self.recvData.lastok && self.recvData.header[head.seq] == nil {
				self.num_recv_data++
				self.recvData.header[head.seq] = head
				if head.seq > self.recvData.maxok {
					self.recvData.maxok = head.seq
				} else {
					head1 := &UdpHeader{}
					head1.seq = head.seq
					n, _ := self.sendAck(head1)
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
				self.sendAck(head1)
				self.num_waste++
				fmt.Println("收到过期数据包", head.seq, head.datasize, self.recvData.lastok, self.num_waste)
			}
		case <-timersec.C:
			fmt.Println(fmt.Sprintf("完成确认序号:%5d,最大确认序号:%5d,接收包:%5d,发送包:%5d,重发包:%5d,超时包:%5d,接受ACKLIST:%5d,接受ACK:%5d,发送ACKLIST:%5d,发送ACK:%5d,重复接收包:%5d,最新PING:%5d", self.sendData.lastok, self.sendData.maxok, self.num_recv_data, self.num_send, self.num_resend, self.num_timeout, self.num_recv_acklist, self.num_recv_ack, self.num_acklist, self.num_ack, self.num_waste, self.ping))
		case <-timercheckack.C:
			self.CheckReSendAck()
		case <-timerack.C:
			if self.recvData.curack < self.recvData.lastok {
				head := &UdpHeader{}
				head.seq = self.recvData.lastok
				head.bitmask |= 1
				self.FillLastokAckHead(head)
				n, _ := self.sendAck(head)
				if n != 0 {
					self.num_acklist++
					self.recvData.curack = self.recvData.lastok
					self.recvData.header[self.recvData.curack].seq = 0
					if self.recvData.lastok-self.recvData.curack >= 3 {
						self.last_ack = head
						timercheckack.Reset(time.Millisecond * 10)
					}
				}
			}
			for i := self.recvData.curack; i <= self.recvData.maxok; i++ {
				if self.recvData.header[i] != nil && self.recvData.header[i].seq != 0 {
					head := &UdpHeader{}
					head.seq = self.recvData.header[i].seq
					if i == self.recvData.maxok {
						self.FillMaxokAckHead(head)
					} else {
						self.FillOkAckHead(head)
					}
					n, _ := self.sendAck(head)
					if n != 0 {
						self.recvData.header[i].seq = 0
						self.num_ack++
					}
				}
			}
		case <-timersend.C:
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			self.CheckSendLostMsg()
			cansend := true
			for i := self.sendData.lastok; i <= self.sendData.curseq; i++ {
				if self.sendData.header[i] != nil && self.sendData.header[i].time_ack == 0 {
					//fmt.Println("检测超时", now-self.sendData.header[i].time_send)
					timeout := self.sendData.header[i].time_send + self.ping + 100
					if now > timeout {
						//发现有更新的包已经确认,所有老包直接重发
						n, _ := self.sendMsg(self.sendData.header[i])
						if n == 0 {
							cansend = true
							break
						}
						self.num_timeout++
						fmt.Println("超时重发", i, self.sendData.lastok, self.sendData.maxok, self.sendData.curseq, self.num_timeout, now-timeout+self.ping+100)
						self.sendData.header[i].time_send = now + self.ping
					}
				}
			}
			if self.Test && cansend {
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
			if head.datasize > 0 && (head.bitmask&1 == 0) {
				self.recvDataCh <- head
			} else {
				self.recvAckCh <- head
			}
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
