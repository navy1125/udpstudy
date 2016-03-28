package udp

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

type TcpTask struct {
	conn             *net.TCPConn
	addr             *net.TCPAddr
	recvData         *UdpData
	sendData         *UdpData
	wait             *bytes.Buffer
	recvDataCh       chan *UdpHeader
	recvAckCh        chan *UdpHeader
	last_ack         *UdpHeader
	last_ack_times   int
	Test             bool
	is_server        bool
	num_resend       int
	num_waste        int
	num_recv_data    int
	num_recv_data2   int
	num_recv_ack     int
	num_recv_acklist int
	num_send         int
	num_ack          int
	num_acklist      int
	num_timeout      int
	ping             int64
	ping_max         int64
	ping_max_seq     uint16
}

func NewTcpTask() *TcpTask {
	task := &TcpTask{
		recvData:   &UdpData{},
		sendData:   &UdpData{},
		recvDataCh: make(chan *UdpHeader, 1024),
		recvAckCh:  make(chan *UdpHeader, 1024),
		ping:       100,
	}
	return task
}

func (self *TcpTask) CheckSendWaitData() bool {
	if self.wait == nil {
		return true
	}
	wait := self.wait
	self.wait = nil
	return self.SendData(wait.Bytes())
	return true
}

func (self *TcpTask) SendData(b []byte) bool {
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
		offset := 0
		if bsize >= cur+255 {
			head.data = b[cur : cur+255]
			offset = 255
		} else {
			head.data = b[cur:bsize]
			offset = bsize - cur
		}
		//self.conn.SetWriteDeadline(time.Now().Add(time.Duration(1 * 2 * int64(time.Second))))
		n, err := self.sendMsg(head, false)
		if err != nil {
			fmt.Println("消息发送失败1", self.sendData.curseq, n, err)
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
		self.sendData.header[head.seq] = head
		self.sendData.curseq++
		if self.sendData.curseq == 65535 {
			self.sendData.curseq = 0
		}
	}
	return true
}
func (self *TcpTask) sendMsg(head *UdpHeader, force bool) (int, error) {
	n, err := self.conn.Write(head.Serialize())
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
func (self *TcpTask) sendAck(head *UdpHeader) (int, error) {
	return 1, nil
	//n, _, err := self.conn.WriteMsgUDP(head.Serialize(), nil, self.addr)
	//return n, err
}
func (self *TcpTask) CheckReSendAck() {
	if self.last_ack != nil {
		self.FillLastokAckHead(self.last_ack)
		//n, _, _ := self.conn.WriteMsgUDP(self.last_ack.Serialize(), nil, self.addr)
		//if n != 0 {
		if 1 != 0 {
			if self.last_ack_times == 0 {
				fmt.Println("CheckReSendAck", self.last_ack.seq)
				self.last_ack = nil
			} else {
				self.last_ack_times--
			}
			self.num_acklist++
		}
	}
}
func (self *TcpTask) FillMaxokAckHead(head *UdpHeader) {
	head.bitmask = 0
	next := head.seq - (head.seq%2)*7 - 1
	for i := uint16(1); i <= 7; i++ {
		if self.recvData.header[next] != nil {
			head.bitmask = set_state(head.bitmask, i)
		}
		next--
	}
}
func (self *TcpTask) FillLastokAckHead(head *UdpHeader) {
	head.datasize = 0
	next := head.seq + 1 + 1 //不算当前,不算下一个,因为下一个必定是nil
	for i := uint16(0); i <= 7; i++ {
		if next < self.recvData.maxok {
			if self.recvData.header[next] != nil {
				head.datasize = set_state(head.datasize, i)
			}
		} else {
			break
		}
		next++
	}
	for i := uint16(1); i <= 7; i++ {
		if next < self.recvData.maxok {
			if self.recvData.header[next] != nil {
				head.bitmask = set_state(head.bitmask, i)
			}
		} else {
			break
		}
		next++
	}
}
func (self *TcpTask) Loop() {
	timersend := time.NewTicker(time.Millisecond * 5)
	timerack := time.NewTicker(time.Millisecond * 20)
	timersec := time.NewTicker(time.Second)
	timercheckack := time.NewTimer(time.Millisecond * 10)
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for {
		select {
		case head := <-self.recvAckCh:
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			//fmt.Println("收确认包", now,head.seq)
			if head.seq >= self.sendData.maxok {
				self.sendData.maxok = head.seq
				if self.sendData.header[head.seq] != nil && self.sendData.header[head.seq].timeout_times == 0 && self.sendData.header[head.seq].lost_times == 0 {
					self.ping = now - self.sendData.header[head.seq].time_send
					if self.ping < 0 {
						fmt.Println("PING值错误:", now, head.seq, now, self.sendData.header[head.seq].time_send, self.ping)
					}
				}
			} else if self.ping > 1000 && self.sendData.header[head.seq] != nil && self.ping < now-self.sendData.header[head.seq].time_send {
				self.ping = now - self.sendData.header[head.seq].time_send
			}
			if head.bitmask&1 == 1 {
				self.num_recv_acklist++
				//fmt.Println("批量确认包完成", now, self.sendData.lastok, head.seq, self.sendData.maxok, head.datasize, head.bitmask)
				for i := self.sendData.lastok; i <= head.seq; i++ {
					self.sendData.header[i] = nil
					if self.ping_max_seq == i {
						self.ping_max = self.ping
						self.ping_max_seq = self.sendData.maxok
					}
				}
				next := head.seq + 1 + 1
				if head.datasize != 0 {
					for i := uint16(0); i <= 7; i++ {
						if isset_state(head.datasize, i) {
							if self.sendData.header[next] != nil && self.sendData.header[next].time_ack == 0 {
								//fmt.Println("last datasize补漏成功:", now, head.seq, next, head.datasize)
								self.sendData.header[next].time_ack = now
							}
						}
						next++
					}
				} else {
					next += 8
				}
				if (head.bitmask >> 1) != 0 {
					for i := uint16(1); i <= 7; i++ {
						if isset_state(head.bitmask, i) {
							if self.sendData.header[next] != nil && self.sendData.header[next].time_ack == 0 {
								//fmt.Println("last bitmask补漏成功:", now, head.seq, next, head.bitmask)
								self.sendData.header[next].time_ack = now
							}
						}
						next++
					}
				}
			} else {
				self.num_recv_ack++
				//fmt.Println(fmt.Sprintf("收到单个SEQ:%5d,完成SEQ:%5d,最大SEQ:%5d,%5d,%5d", head.seq, self.sendData.lastok, self.sendData.maxok, head.seq-(head.seq%2)*7-1, head.bitmask))
				if self.sendData.header[head.seq] != nil {
					self.sendData.header[head.seq].time_ack = now
					//self.sendData.header[head.seq] = nil
				}
				if head.bitmask != 0 {
					next := head.seq - (head.seq%2)*7 - 1
					for i := uint16(1); i <= 7; i++ {
						if isset_state(head.bitmask, i) {
							if self.sendData.header[next] != nil && self.sendData.header[next].time_ack == 0 {
								//fmt.Println("max bitmask补漏成功:", now, head.seq, next, head.bitmask)
								self.sendData.header[next].time_ack = now
							}
						}
						next--
					}
				}
			}
		case head := <-self.recvDataCh:
			//fmt.Println("收包:", head.seq, self.recvData.lastok, self.num_recv_data)
			//self.Test = true
			if head.seq >= self.recvData.lastok && self.recvData.header[head.seq] == nil {
				if (head.bitmask & 2) == 2 {
					self.num_recv_data2++
					//fmt.Println(fmt.Sprintf("重发有效SEQ:%5d,数据大小:%3d,完成SEQ:%5d,浪费数量:%5d", head.seq, head.datasize, self.recvData.lastok, self.num_waste))
				}
				self.num_recv_data++
				self.recvData.header[head.seq] = head
				if head.seq >= self.recvData.maxok {
					self.recvData.maxok = head.seq
				} else {
					head1 := &UdpHeader{}
					head1.seq = head.seq
					self.FillMaxokAckHead(head1)
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
				//过期数据,说明对方没有收到确认包,发一个
				head1 := &UdpHeader{}
				if head1.seq <= self.recvData.lastok {
					head1.bitmask |= 1
					head1.seq = self.recvData.lastok
					self.FillLastokAckHead(head1)
				} else if head1.seq == self.recvData.lastok+1 {
					head1.bitmask |= 1
					head1.seq = self.recvData.lastok + 1
					self.FillLastokAckHead(head1)
				} else {
					head1.seq = head.seq
					self.FillMaxokAckHead(head1)
				}
				self.sendAck(head1)
				self.num_waste++
				fmt.Println(fmt.Sprintf("收到过期SEQ:%5d,数据大小:%3d,完成SEQ:%5d,浪费数量:%5d", head.seq, head.datasize, self.recvData.lastok, self.num_waste))
			}
		case <-timersec.C:
			fmt.Println(fmt.Sprintf("TCPSEQ:%5d,最大SEQ:%5d,接包:%5d,重接包:%5d,发包:%5d,重发包:%5d,超时包:%d,接受ACKLIST:%5d,接受ACK:%5d,发送ACKLIST:%5d,发送ACK:%5d,重收包:%5d,PING:%4d,PING_MAX:%4d:%d", self.sendData.lastok, self.sendData.maxok, self.num_recv_data, self.num_recv_data2, self.num_send, self.num_resend, self.num_timeout, self.num_recv_acklist, self.num_recv_ack, self.num_acklist, self.num_ack, self.num_waste, self.ping, self.ping_max, self.ping_max_seq))
		case <-timercheckack.C:
			self.CheckReSendAck()
			if self.last_ack_times != 0 {
				timercheckack.Reset(time.Millisecond * 10)
			}
			//fmt.Println("timercheckack")
		case <-timerack.C:
			if self.recvData.curack < self.recvData.lastok {
				head := &UdpHeader{}
				head.seq = self.recvData.lastok
				head.bitmask |= 1
				self.FillLastokAckHead(head)
				n, _ := self.sendAck(head)
				if n != 0 {
					self.num_acklist++
					if self.recvData.lastok-self.recvData.curack >= 3 {
						self.last_ack = head
						self.last_ack_times = int((self.recvData.lastok-self.recvData.curack)/64) + 1
						timercheckack.Reset(time.Millisecond * 10)
					}
					self.recvData.curack = self.recvData.lastok
					self.recvData.header[self.recvData.curack].seq = 0
				}
			}
			for i := self.recvData.curack; i <= self.recvData.maxok; i++ {
				if self.recvData.header[i] != nil && self.recvData.header[i].seq != 0 {
					head := &UdpHeader{}
					head.seq = self.recvData.header[i].seq
					self.FillMaxokAckHead(head)
					n, _ := self.sendAck(head)
					if n != 0 {
						self.recvData.header[i].seq = 0
						self.num_ack++
					}
				}
			}
		case <-timersend.C:
			cansend := true
			/*
				now = int64(time.Now().UnixNano() / int64(time.Millisecond))
				for i := self.sendData.lastok; i <= self.sendData.curseq; i++ {
					if self.sendData.header[i] != nil && self.sendData.header[i].time_ack == 0 && self.sendData.header[i].lost_times == 0 {
						//fmt.Println("检测超时", now-self.sendData.header[i].time_send)
						timeout := self.sendData.header[i].time_send + (self.sendData.header[i].timeout_times+1)*self.ping + 100
						if now > timeout {
							//发现有更新的包已经确认,所有老包直接重发
							n, _ := self.sendMsg(self.sendData.header[i],true)
							if n == 0 {
								cansend = true
								break
							}
							self.num_timeout++
							fmt.Println("超时重发", i, self.sendData.lastok, self.sendData.maxok, self.sendData.curseq, self.num_timeout, now-timeout+self.ping+100, self.ping)
							self.sendData.header[i].timeout_times++
							self.sendData.header[i].time_send = now
						}
					}
				}
				// */
			if self.Test && cansend {
				if self.sendData.maxok < 65534 {
					self.SendData([]byte("wanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghai"))
				}
			}
			self.CheckSendWaitData()
		}
	}
}
func (self *TcpTask) LoopRecv() {
	b := make([]byte, 1024)
	left := 0
	for {
		n, err := self.conn.Read(b[left:])
		if err != nil {
			fmt.Println("ERROR: ", err, n)
			return
		}
		all := n + left
		for all >= HEADLEN {
			head := &UdpHeader{}
			offset := head.Unserialize(b, all)
			if offset > 0 {
				//fmt.Println("ReadFromUDP: ", left, n, all, int(head.datasize))
				copy(b, b[offset:all])
				all -= offset
			} else {
				//fmt.Println("self.conn.Read: ", left, n, all, int(head.datasize))
				break
			}
			if head.datasize > 0 && (head.bitmask&1 == 0) {
				self.recvDataCh <- head
			} else {
				self.recvAckCh <- head
			}
		}
		left = all
	}
}
func (self *TcpTask) Dial(addr *net.TCPAddr) (err error) {
	self.conn, err = net.DialTCP("tcp", nil, addr)
	return err
}
func (self *TcpTask) Listen(addr *net.TCPAddr) (err error) {
	listen, err := net.ListenTCP("tcp", addr)
	self.conn, err = listen.AcceptTCP()
	if err != nil {
		fmt.Println("net.ListenTCP", err)
	}
	self.is_server = true
	return err
}
