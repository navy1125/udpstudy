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

type UdpFrame struct {
	datasize      byte
	bitmask       byte
	seq           uint16
	time_send     int64
	time_recv     int64
	time_ack      int64
	lost_times    int64
	timeout_times int64
	data          []byte
}
type SendUdpFrame struct {
	UdpFrame
	time_send     int64
	time_ack      int64
	lost_times    int64
	timeout_times int64
}
type RecvUdpFrame struct {
	UdpFrame
	time_recv int64
}

func (self *UdpFrame) Serialize() []byte {
	datasize := byte(len(self.data))
	if datasize != 0 {
		self.datasize = datasize
	}
	buf := bytes.NewBuffer(make([]byte, 0, self.GetHeadSize()+int(datasize)))
	buf.WriteByte(self.datasize)
	buf.WriteByte(self.bitmask)
	binary.Write(buf, binary.LittleEndian, self.seq)
	buf.Write(self.data)
	return buf.Bytes()
}

func (self *UdpFrame) Unserialize(b []byte, all int) int {
	self.datasize = b[0]
	self.bitmask = b[1]
	seq := b[2:self.GetHeadSize()]
	self.seq = uint16((int(seq[1]) << 8) + int(seq[0]))
	datasize := 0
	if self.seq != 0 && self.bitmask&1 == 0 {
		datasize = int(self.datasize)
	}
	if all < datasize+self.GetHeadSize() {
		//fmt.Println("Unserialize: ", all, datasize)
		return 0
	}
	if datasize > 0 {
		self.data = b[self.GetHeadSize() : self.GetHeadSize()+datasize]
	}
	return self.GetAllSize()
}
func (self *UdpFrame) GetAllSize() int {
	return self.GetDataSize() + self.GetHeadSize()
}
func (self *UdpFrame) GetDataSize() int {
	return len(self.data)
}
func (self *UdpFrame) GetHeadSize() int {
	return HEADLEN
}

type SendUdpBuff struct {
	curseq           uint16
	lastok           uint16
	maxok            uint16
	header           [65536]*SendUdpFrame
	wait             *bytes.Buffer
	num_send         int
	num_resend       int
	num_recv_ack     int
	num_recv_acklist int
}

type RecvUdpBuff struct {
	curack         uint16
	lastok         uint16
	maxok          uint16
	header         [65536]*RecvUdpFrame
	num_recv_waste int
	num_recv_data  int
	num_recv_data2 int
	num_ack        int
	num_acklist    int
}

type UdpTask struct {
	conn_tcp     *net.TCPConn
	conn_udp     *net.UDPConn
	addr         *net.UDPAddr
	sendBuff     *SendUdpBuff
	recvBuff     *RecvUdpBuff
	recvBuffCh   chan *RecvUdpFrame
	recvAckCh    chan *RecvUdpFrame
	Test         bool
	is_server    bool
	num_timeout  int
	ping         int64
	ping_max     int64
	ping_max_seq uint16
	resendmax    uint16
	timeout_seq  uint16
}

func NewUdpTask() *UdpTask {
	task := &UdpTask{
		recvBuff: &RecvUdpBuff{
			lastok: 1,
			maxok:  1,
			curack: 1,
		},
		sendBuff: &SendUdpBuff{
			curseq: 1,
			lastok: 1,
			maxok:  1,
		},
		recvBuffCh: make(chan *RecvUdpFrame, 1024),
		recvAckCh:  make(chan *RecvUdpFrame, 1024),
		ping:       100,
	}
	return task
}

func (self *UdpTask) CheckSendWaitData() bool {
	if self.sendBuff.wait == nil {
		return true
	}
	wait := self.sendBuff.wait
	self.sendBuff.wait = nil
	return self.SendData(wait.Bytes())
	return true
}

func (self *UdpTask) SendData(b []byte) bool {
	bsize := len(b)
	for cur := 0; cur < bsize; {
		if self.sendBuff.curseq+1 == self.sendBuff.lastok {
			if self.sendBuff.wait == nil {
				self.sendBuff.wait = bytes.NewBuffer(nil)
			}
			self.sendBuff.wait.Write(b[cur:])
			return false
		}
		head := &SendUdpFrame{}
		head.seq = self.sendBuff.curseq
		offset := 0
		if bsize >= cur+255 {
			head.data = b[cur : cur+255]
			offset = 255
		} else {
			head.data = b[cur:bsize]
			offset = bsize - cur
		}
		self.conn_udp.SetWriteDeadline(time.Now().Add(time.Duration(1 * 2 * int64(time.Second))))
		n, err := self.sendMsg(head, false)
		if err != nil {
			fmt.Println("消息发送失败2", self.sendBuff.curseq, n, err)
		}
		if n == 0 {
			if self.sendBuff.wait == nil {
				self.sendBuff.wait = bytes.NewBuffer(nil)
			}
			self.sendBuff.wait.Write(b[cur:])
			return false
		} else {
			cur += offset
		}
		self.sendBuff.header[head.seq] = head
		if self.sendBuff.curseq == 65534 {
			return false
		}
		self.sendBuff.curseq++
		if self.sendBuff.curseq == 65535 {
			self.sendBuff.curseq = 1
			//self.sendBuff.curseq = 0
		}
	}
	return true
}
func (self *UdpTask) sendMsg(head *SendUdpFrame, force bool) (int, error) {
	if head.seq == 0 {
		panic("ddd")
	}
	//if head.seq-self.sendBuff.lastok >= 100 && head.seq != self.sendBuff.lastok+1 {
	if head.seq-self.sendBuff.lastok >= 1024 && force == false {
		//fmt.Println("缓冲区满,等待下次发送", head.seq, self.sendBuff.curseq, self.sendBuff.lastok, self.sendBuff.maxok)
		return 0, nil
	}
	if head.seq-self.sendBuff.lastok >= 1000 && self.ping_max < 5000 && force == true {
		//fmt.Println("缓冲区满要满了,等待下次发送", head.seq, self.sendBuff.curseq, self.sendBuff.lastok, self.sendBuff.maxok)
	}
	n, _, err := self.conn_udp.WriteMsgUDP(head.Serialize(), nil, self.addr)
	if n == 0 {
		if err == nil {
			panic("ddddd")
		}
	} else {
		head.time_send = int64(time.Now().UnixNano() / time.Millisecond.Nanoseconds())
		//fmt.Println("消息发送", self.sendBuff.curseq, n, err)
	}
	self.sendBuff.num_send++
	return n, err
}
func (self *UdpTask) sendAck(head *UdpFrame) (int, error) {
	if head.seq != 0 && head.datasize > 0 && (head.bitmask&1 == 0) {
		fmt.Println("sendAck", head.seq, head.datasize, head.bitmask)
		panic("ddd")
	}
	n, err := self.conn_tcp.Write(head.Serialize())
	if n != head.GetAllSize() {
		panic("ddd")
	}
	return n, err
}
func (self *UdpTask) sendMsgTCP(head *UdpFrame) (int, error) {
	n, err := self.conn_tcp.Write(head.Serialize())
	return n, err
}
func (self *UdpTask) CheckSendLostMsg() {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for i := self.sendBuff.lastok; i < uint16(self.resendmax); i++ {
		//fmt.Println("尝试丢包重发", i, self.sendBuff.lastok, self.sendBuff.maxok)
		if self.sendBuff.header[i] != nil {
			diff := now - self.sendBuff.header[i].time_send
			if diff >= self.ping*(int64(i-self.sendBuff.lastok)+self.sendBuff.header[i].lost_times+1)+int64(i-self.sendBuff.lastok)*30 {
				//发现有更新的包已经确认,所有老包直接重发
				self.sendBuff.header[i].bitmask |= 2
				//oldtime := self.sendBuff.header[i].time_send
				n, _ := self.sendMsg(self.sendBuff.header[i], true)
				//fmt.Println("丢包重发", now, i, self.sendBuff.lastok, self.resendmax, self.sendBuff.maxok, now, diff)
				if n == 0 {
					break
				}
				self.sendBuff.header[i].time_send = now
				self.sendBuff.header[i].lost_times++
				self.sendBuff.num_resend++
			}
			if diff > self.ping_max {
				self.ping_max = diff
				self.ping_max_seq = i
			}
		}
	}
}
func (self *UdpTask) CheckLastok() {
	//tmp := self.sendBuff.lastok
	for i := self.sendBuff.lastok; i <= self.sendBuff.maxok; i++ {
		if self.sendBuff.header[i] != nil {
			if self.sendBuff.maxok-self.sendBuff.lastok >= 102 {
				//fmt.Println("等待乱序确认异常", int64(time.Now().UnixNano()/int64(time.Millisecond)), self.sendBuff.lastok, self.sendBuff.maxok)
			}
			break
		}
		self.sendBuff.lastok = i
	}
	/*
		if self.sendBuff.lastok != tmp {
			fmt.Println(fmt.Sprintf("完成SEQ增加:%5d,当前完成SEQ:%5d,最大SEQ:%5d", tmp, self.sendBuff.lastok, self.sendBuff.maxok))
		}
		// */
}
func isset_state(state byte, teststate uint16) bool {
	//return 0 != (state[teststate/8] & (0xff & (1 << (teststate % 8))))
	return 0 != (state & (0xff & (1 << (teststate % 8))))
}

func set_state(state byte, teststate uint16) byte {
	state |= (0xff & (1 << (teststate % 8)))
	return state
}
func (self *UdpTask) FillMaxokAckHead(head *UdpFrame) (timeout_seq uint16, need bool) {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	head.bitmask = 0
	next := head.seq + 1
	for i := uint16(1); i <= 7; i++ {
		if self.recvBuff.header[next] != nil {
			head.bitmask = set_state(head.bitmask, i)
			if now-self.recvBuff.header[next].time_recv >= 10 {
				timeout_seq = next
			}
			self.recvBuff.header[next].seq = 0
		} else {
			need = true
		}
		next++
	}
	return
}
func (self *UdpTask) FillLastokAckHead(head *UdpFrame) (timeout_seq uint16, need bool) {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	head.datasize = 0
	next := head.seq + 1 + 1 //不算当前,不算下一个,因为下一个必定是nil
	for i := uint16(0); i <= 7; i++ {
		if next < self.recvBuff.maxok {
			if self.recvBuff.header[next] != nil {
				head.datasize = set_state(head.datasize, i)
				if now-self.recvBuff.header[next].time_recv >= 10 {
					timeout_seq = next
				}
				self.recvBuff.header[next].seq = 0
			} else {
				need = true
			}
		} else {
			break
		}
		next++
	}
	for i := uint16(1); i <= 7; i++ {
		if next < self.recvBuff.maxok {
			if self.recvBuff.header[next] != nil {
				head.bitmask = set_state(head.bitmask, i)
				if now-self.recvBuff.header[next].time_recv >= 10 {
					timeout_seq = next
				}
				self.recvBuff.header[next].seq = 0
			} else {
				need = true
			}
		} else {
			break
		}
		next++
	}
	return
}
func (self *UdpTask) Loop() {
	timersend := time.NewTicker(time.Millisecond * 5)
	timerack := time.NewTicker(time.Millisecond * 20)
	timersec := time.NewTicker(time.Second)
	timercheckack := time.NewTimer(time.Millisecond * 10)
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for {
		select {
		case head := <-self.recvAckCh:
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			if head.seq == 0 {
				self.resendmax = uint16(head.bitmask)<<8 + uint16(head.datasize)
				//fmt.Println("收到请求重发", now, self.resendmax, head.bitmask, head.datasize)
				self.CheckSendLostMsg()
			} else {
				//ismax := false
				if head.seq >= self.sendBuff.maxok {
					//ismax = true
					self.sendBuff.maxok = head.seq
					if self.sendBuff.header[head.seq] != nil && self.sendBuff.header[head.seq].timeout_times == 0 && self.sendBuff.header[head.seq].lost_times == 0 {
						self.ping = now - self.sendBuff.header[head.seq].time_send
						if self.ping < 0 {
							fmt.Println("PING值错误:", now, head.seq, now, self.sendBuff.header[head.seq].time_send, self.ping)
						}
					}
				} else if self.ping > 1000 && self.sendBuff.header[head.seq] != nil && self.ping < now-self.sendBuff.header[head.seq].time_send {
					self.ping = now - self.sendBuff.header[head.seq].time_send
				}
				if head.bitmask&1 == 1 {
					self.sendBuff.num_recv_acklist++
					//fmt.Println("批量确认包完成", now, self.sendBuff.lastok, head.seq, self.sendBuff.maxok, head.datasize, head.bitmask)
					for i := self.sendBuff.lastok; i <= head.seq; i++ {
						self.sendBuff.header[i] = nil
						if self.ping_max_seq == i {
							self.ping_max = self.ping
							self.ping_max_seq = self.sendBuff.maxok
						}
					}
					next := head.seq + 1 + 1
					if head.datasize != 0 {
						for i := uint16(0); i <= 7; i++ {
							if isset_state(head.datasize, i) {
								if self.sendBuff.header[next] != nil {
									//fmt.Println("last datasize补漏成功:", now, head.seq, next, head.datasize)
									self.sendBuff.header[next] = nil
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
								if self.sendBuff.header[next] != nil {
									//fmt.Println("last bitmask补漏成功:", now, head.seq, next, head.bitmask)
									self.sendBuff.header[next] = nil
								}
							}
							next++
						}
					}
				} else {
					self.sendBuff.num_recv_ack++
					//fmt.Println(fmt.Sprintf("收到单个SEQ:%5d,完成SEQ:%5d,最大SEQ:%5d,%5d,%5d", head.seq, self.sendBuff.lastok, self.sendBuff.maxok, head.seq-(head.seq%2)*7-1, head.bitmask))
					if self.sendBuff.header[head.seq] != nil {
						self.sendBuff.header[head.seq] = nil
						//self.sendBuff.header[head.seq] = nil
					}
					if head.bitmask != 0 {
						next := head.seq + 1
						//next := head.seq - (head.seq%2)*7 - 1
						for i := uint16(1); i <= 7; i++ {
							if isset_state(head.bitmask, i) {
								if self.sendBuff.header[next] != nil {
									//fmt.Println("max bitmask补漏成功:", now, head.seq, next, head.bitmask)
									self.sendBuff.header[next] = nil
								}
							}
							next++
						}
					}
				}
			}
			self.CheckLastok()
		case head := <-self.recvBuffCh:
			self.Test = true
			if head.seq >= self.recvBuff.lastok && self.recvBuff.header[head.seq] == nil {
				if (head.bitmask & 2) == 2 {
					self.recvBuff.num_recv_data2++
					//fmt.Println(fmt.Sprintf("重发有效SEQ:%5d,数据大小:%3d,完成SEQ:%5d,浪费数量:%5d", head.seq, head.datasize, self.recvBuff.lastok, self.recvBuff.num_recv_waste))
				}
				self.recvBuff.num_recv_data++
				head.time_recv = now
				self.recvBuff.header[head.seq] = head
				if head.seq >= self.recvBuff.maxok {
					self.recvBuff.maxok = head.seq
				}
				//fmt.Println(head.seq, self.recvBuff.lastok, self.recvBuff.maxok)
			} else {
				//过期数据,说明对方没有收到确认包,发一个
				self.recvBuff.num_recv_waste++
				//fmt.Println(fmt.Sprintf("收到过期SEQ:%5d,数据大小:%3d,完成SEQ:%5d,浪费数量:%5d", head.seq, head.datasize, self.recvBuff.lastok, self.recvBuff.num_recv_waste))
			}
		case <-timersec.C:
			fmt.Println(fmt.Sprintf("完成SEQ:%5d,最大SEQ:%5d,接包:%5d,重接包:%5d,发包:%5d,重发包:%5d,超时包:%d,接受ACKLIST:%5d,接受ACK:%5d,发送ACKLIST:%5d,发送ACK:%5d,重收包:%5d,PING:%4d,PING_MAX:%4d:%d", self.sendBuff.lastok, self.sendBuff.maxok, self.recvBuff.num_recv_data, self.recvBuff.num_recv_data2, self.sendBuff.num_send, self.sendBuff.num_resend, self.num_timeout, self.sendBuff.num_recv_acklist, self.sendBuff.num_recv_ack, self.recvBuff.num_acklist, self.recvBuff.num_ack, self.recvBuff.num_recv_waste, self.ping, self.ping_max, self.ping_max_seq))
		case <-timercheckack.C:
			//fmt.Println("timercheckack")
		case <-timerack.C:
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			for i := self.recvBuff.lastok; i <= self.recvBuff.maxok; i++ {
				if self.recvBuff.header[i] == nil {
					break
				}
				self.recvBuff.lastok = i
			}
			timeout_seq := uint16(0)
			timeou_need := false
			if self.recvBuff.curack < self.recvBuff.lastok {
				head := &UdpFrame{}
				head.seq = self.recvBuff.lastok
				head.bitmask |= 1
				tmp, need := self.FillLastokAckHead(head)
				if tmp > 0 && need {
					timeout_seq = tmp
					timeou_need = true
				}
				self.sendAck(head)
				self.recvBuff.num_acklist++
				self.recvBuff.curack = self.recvBuff.lastok
				self.recvBuff.header[self.recvBuff.curack].seq = 0
			}
			if self.recvBuff.header[self.recvBuff.maxok] != nil {
				for i := self.recvBuff.curack; i <= self.recvBuff.maxok; i++ {
					if self.recvBuff.header[i] != nil {
						if now-self.recvBuff.header[i].time_recv >= 10 {
							timeout_seq = i
						}
						if self.recvBuff.header[i].seq != 0 {
							head := &UdpFrame{}
							head.seq = self.recvBuff.header[i].seq
							tmp, need := self.FillMaxokAckHead(head)
							if tmp > 0 && need {
								timeout_seq = tmp
								timeou_need = true
							}
							self.sendAck(head)
							self.recvBuff.header[i].seq = 0
							self.recvBuff.num_ack++
						}
					} else {
						timeou_need = true
					}
				}
			}
			if timeout_seq > 0 && timeou_need && timeout_seq != self.timeout_seq {
				head := &UdpFrame{}
				head.seq = 0
				head.bitmask = byte(timeout_seq >> 8)
				head.datasize = byte(timeout_seq & 0xff)
				fmt.Println("请求重发", now, timeout_seq, head.bitmask, head.datasize)
				self.timeout_seq = timeout_seq
				self.sendAck(head)
			}
		case <-timersend.C:
			self.CheckSendLostMsg()
			cansend := true
			/*
				now = int64(time.Now().UnixNano() / int64(time.Millisecond))
				for i := self.sendBuff.lastok; i <= self.sendBuff.curseq; i++ {
					if self.sendBuff.header[i] != nil && self.sendBuff.header[i].time_ack == 0 && self.sendBuff.header[i].lost_times == 0 {
						//fmt.Println("检测超时", now-self.sendBuff.header[i].time_send)
						timeout := self.sendBuff.header[i].time_send + (self.sendBuff.header[i].timeout_times+1)*self.ping + 100
						if now > timeout {
							//发现有更新的包已经确认,所有老包直接重发
							n, _ := self.sendMsg(self.sendBuff.header[i],true)
							if n == 0 {
								cansend = true
								break
							}
							self.num_timeout++
							fmt.Println("超时重发", i, self.sendBuff.lastok, self.sendBuff.maxok, self.sendBuff.curseq, self.num_timeout, now-timeout+self.ping+100, self.ping)
							self.sendBuff.header[i].timeout_times++
							self.sendBuff.header[i].time_send = now
						}
					}
				}
				// */
			if self.Test && cansend {
				if self.sendBuff.maxok < 65534 {
					self.SendData([]byte("wanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghaijunwanghai"))
				}
			}
			self.CheckSendWaitData()
		}
	}
}
func (self *UdpTask) LoopRecvUDP() {
	b := make([]byte, 1024)
	left := 0
	for {
		n, addr, err := self.conn_udp.ReadFromUDP(b[left:])
		if err != nil {
			fmt.Println("ERROR: ", err, n, addr)
			return
		}
		if self.is_server {
			self.addr = addr
		}
		all := n + left
		for all >= HEADLEN {
			head := &RecvUdpFrame{}
			offset := head.Unserialize(b, all)
			if offset > 0 {
				//fmt.Println("ReadFromUDP: ", left, n, all, int(head.datasize))
				copy(b, b[offset:all])
				all -= offset
			} else {
				break
			}
			if head.seq != 0 && head.datasize > 0 && (head.bitmask&1 == 0) {
				self.recvBuffCh <- head
			} else {
				self.recvAckCh <- head
			}
		}
		left = all
	}
}
func (self *UdpTask) LoopRecvTCP() {
	b := make([]byte, 1024)
	left := 0
	for {
		n, err := self.conn_tcp.Read(b[left:])
		if err != nil {
			fmt.Println("ERROR: ", err, n)
			return
		}
		all := n + left
		for all >= HEADLEN {
			head := &RecvUdpFrame{}
			offset := head.Unserialize(b, all)
			if offset > 0 {
				//fmt.Println("LoopRecvTCP: ", left, n, all, head.seq, head.datasize, head.bitmask, head.GetAllSize())
				copy(b, b[offset:all])
				all -= offset
			} else {
				break
			}
			if head.seq != 0 && head.datasize > 0 && (head.bitmask&1 == 0) {
				if !self.is_server {
					//fmt.Println("LoopRecvTCP: ", left, n, all, head.GetAllSize())
				}
				self.recvBuffCh <- head
			} else {
				self.recvAckCh <- head
			}
		}
		left = all
	}
}
func (self *UdpTask) Dial(addr_udp *net.UDPAddr, addr_tcp *net.TCPAddr) (err error) {
	self.conn_udp, err = net.DialUDP("udp", nil, addr_udp)
	if err == nil {
		self.conn_tcp, err = net.DialTCP("tcp", nil, addr_tcp)
	}
	return err
}
func (self *UdpTask) Listen(addr_udp *net.UDPAddr, addr_tcp *net.TCPAddr) (err error) {
	self.conn_udp, err = net.ListenUDP("udp", addr_udp)
	if err == nil {
		listen, err := net.ListenTCP("tcp", addr_tcp)
		self.conn_tcp, err = listen.AcceptTCP()
		if err != nil {
			fmt.Println("net.ListenTCP", err)
		}
	}
	self.is_server = true
	return err
}
