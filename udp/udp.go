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

func (self *UdpHeader) Serialize() []byte {
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

func (self *UdpHeader) Unserialize(b []byte, all int) int {
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
func (self *UdpHeader) GetAllSize() int {
	return self.GetDataSize() + self.GetHeadSize()
}
func (self *UdpHeader) GetDataSize() int {
	return len(self.data)
}
func (self *UdpHeader) GetHeadSize() int {
	return HEADLEN
}

type UdpData struct {
	curseq uint16
	lastok uint16
	maxok  uint16
	curack uint16
	header [65536]*UdpHeader
}

type UdpTask struct {
	conn_tcp         *net.TCPConn
	conn_udp         *net.UDPConn
	addr             *net.UDPAddr
	recvData         *UdpData
	sendData         *UdpData
	wait             *bytes.Buffer
	recvDataCh       chan *UdpHeader
	recvAckCh        chan *UdpHeader
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
	resendmax        uint16
	timeout_seq      uint16
}

func NewUdpTask() *UdpTask {
	task := &UdpTask{
		recvData: &UdpData{
			curseq: 1,
			lastok: 1,
			maxok:  1,
			curack: 1,
		},
		sendData: &UdpData{
			curseq: 1,
			lastok: 1,
			maxok:  1,
			curack: 1,
		},
		recvDataCh: make(chan *UdpHeader, 1024),
		recvAckCh:  make(chan *UdpHeader, 1024),
		ping:       100,
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
			fmt.Println("消息发送失败2", self.sendData.curseq, n, err)
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
		if self.sendData.curseq == 65534 {
			return false
		}
		self.sendData.curseq++
		if self.sendData.curseq == 65535 {
			self.sendData.curseq = 1
			//self.sendData.curseq = 0
		}
	}
	return true
}
func (self *UdpTask) sendMsg(head *UdpHeader, force bool) (int, error) {
	if head.seq == 0 {
		panic("ddd")
	}
	//if head.seq-self.sendData.lastok >= 100 && head.seq != self.sendData.lastok+1 {
	if head.seq-self.sendData.lastok >= 1024 && force == false {
		//fmt.Println("缓冲区满,等待下次发送", head.seq, self.sendData.curseq, self.sendData.lastok, self.sendData.maxok)
		return 0, nil
	}
	if head.seq-self.sendData.lastok >= 1000 && self.ping_max < 5000 && force == true {
		//fmt.Println("缓冲区满要满了,等待下次发送", head.seq, self.sendData.curseq, self.sendData.lastok, self.sendData.maxok)
	}
	n, _, err := self.conn_udp.WriteMsgUDP(head.Serialize(), nil, self.addr)
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
func (self *UdpTask) sendMsgTCP(head *UdpHeader) (int, error) {
	n, err := self.conn_tcp.Write(head.Serialize())
	return n, err
}
func (self *UdpTask) CheckSendLostMsg() {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	for i := self.sendData.lastok; i < uint16(self.resendmax); i++ {
		//fmt.Println("尝试丢包重发", i, self.sendData.lastok, self.sendData.maxok)
		if self.sendData.header[i] != nil {
			diff := now - self.sendData.header[i].time_send
			if diff >= self.ping*(int64(i-self.sendData.lastok)+self.sendData.header[i].lost_times+1)+int64(i-self.sendData.lastok)*30 {
				//发现有更新的包已经确认,所有老包直接重发
				self.sendData.header[i].bitmask |= 2
				//oldtime := self.sendData.header[i].time_send
				n, _ := self.sendMsg(self.sendData.header[i], true)
				//fmt.Println("丢包重发", now, i, self.sendData.lastok, self.resendmax, self.sendData.maxok, now, diff)
				if n == 0 {
					break
				}
				self.sendData.header[i].time_send = now
				self.sendData.header[i].lost_times++
				self.num_resend++
			}
			if diff > self.ping_max {
				self.ping_max = diff
				self.ping_max_seq = i
			}
		}
	}
}
func (self *UdpTask) CheckLastok() {
	//tmp := self.sendData.lastok
	for i := self.sendData.lastok; i <= self.sendData.maxok; i++ {
		if self.sendData.header[i] != nil {
			if self.sendData.maxok-self.sendData.lastok >= 102 {
				//fmt.Println("等待乱序确认异常", int64(time.Now().UnixNano()/int64(time.Millisecond)), self.sendData.lastok, self.sendData.maxok)
			}
			break
		}
		self.sendData.lastok = i
	}
	/*
		if self.sendData.lastok != tmp {
			fmt.Println(fmt.Sprintf("完成SEQ增加:%5d,当前完成SEQ:%5d,最大SEQ:%5d", tmp, self.sendData.lastok, self.sendData.maxok))
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
func (self *UdpTask) FillMaxokAckHead(head *UdpHeader) (timeout_seq uint16, need bool) {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	head.bitmask = 0
	next := head.seq + 1
	for i := uint16(1); i <= 7; i++ {
		if self.recvData.header[next] != nil {
			head.bitmask = set_state(head.bitmask, i)
			if now-self.recvData.header[next].time_recv >= 10 {
				timeout_seq = next
			}
			self.recvData.header[next].seq = 0
		} else {
			need = true
		}
		next++
	}
	return
}
func (self *UdpTask) FillLastokAckHead(head *UdpHeader) (timeout_seq uint16, need bool) {
	now := int64(time.Now().UnixNano() / int64(time.Millisecond))
	head.datasize = 0
	next := head.seq + 1 + 1 //不算当前,不算下一个,因为下一个必定是nil
	for i := uint16(0); i <= 7; i++ {
		if next < self.recvData.maxok {
			if self.recvData.header[next] != nil {
				head.datasize = set_state(head.datasize, i)
				if now-self.recvData.header[next].time_recv >= 10 {
					timeout_seq = next
				}
				self.recvData.header[next].seq = 0
			} else {
				need = true
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
				if now-self.recvData.header[next].time_recv >= 10 {
					timeout_seq = next
				}
				self.recvData.header[next].seq = 0
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
				if self.is_server {
					fmt.Println("收确认包", now, head.seq, head.bitmask, head.datasize)
				}
				//ismax := false
				if head.seq >= self.sendData.maxok {
					//ismax = true
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
								if self.sendData.header[next] != nil {
									//fmt.Println("last datasize补漏成功:", now, head.seq, next, head.datasize)
									self.sendData.header[next] = nil
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
								if self.sendData.header[next] != nil {
									//fmt.Println("last bitmask补漏成功:", now, head.seq, next, head.bitmask)
									self.sendData.header[next] = nil
								}
							}
							next++
						}
					}
				} else {
					self.num_recv_ack++
					//fmt.Println(fmt.Sprintf("收到单个SEQ:%5d,完成SEQ:%5d,最大SEQ:%5d,%5d,%5d", head.seq, self.sendData.lastok, self.sendData.maxok, head.seq-(head.seq%2)*7-1, head.bitmask))
					if self.sendData.header[head.seq] != nil {
						self.sendData.header[head.seq] = nil
						//self.sendData.header[head.seq] = nil
					}
					if head.bitmask != 0 {
						next := head.seq + 1
						//next := head.seq - (head.seq%2)*7 - 1
						for i := uint16(1); i <= 7; i++ {
							if isset_state(head.bitmask, i) {
								if self.sendData.header[next] != nil {
									//fmt.Println("max bitmask补漏成功:", now, head.seq, next, head.bitmask)
									self.sendData.header[next] = nil
								}
							}
							next++
						}
					}
				}
			}
			self.CheckLastok()
		case head := <-self.recvDataCh:
			if !self.is_server {
				fmt.Println("收数据包", now, head.seq, head.bitmask, head.datasize)
			}
			//self.Test = true
			if head.seq >= self.recvData.lastok && self.recvData.header[head.seq] == nil {
				if (head.bitmask & 2) == 2 {
					self.num_recv_data2++
					//fmt.Println(fmt.Sprintf("重发有效SEQ:%5d,数据大小:%3d,完成SEQ:%5d,浪费数量:%5d", head.seq, head.datasize, self.recvData.lastok, self.num_waste))
				}
				self.num_recv_data++
				head.time_recv = now
				self.recvData.header[head.seq] = head
				if head.seq >= self.recvData.maxok {
					self.recvData.maxok = head.seq
				}
				//fmt.Println(head.seq, self.recvData.lastok, self.recvData.maxok)
			} else {
				//过期数据,说明对方没有收到确认包,发一个
				self.num_waste++
				//fmt.Println(fmt.Sprintf("收到过期SEQ:%5d,数据大小:%3d,完成SEQ:%5d,浪费数量:%5d", head.seq, head.datasize, self.recvData.lastok, self.num_waste))
			}
		case <-timersec.C:
			fmt.Println(fmt.Sprintf("完成SEQ:%5d,最大SEQ:%5d,接包:%5d,重接包:%5d,发包:%5d,重发包:%5d,超时包:%d,接受ACKLIST:%5d,接受ACK:%5d,发送ACKLIST:%5d,发送ACK:%5d,重收包:%5d,PING:%4d,PING_MAX:%4d:%d", self.sendData.lastok, self.sendData.maxok, self.num_recv_data, self.num_recv_data2, self.num_send, self.num_resend, self.num_timeout, self.num_recv_acklist, self.num_recv_ack, self.num_acklist, self.num_ack, self.num_waste, self.ping, self.ping_max, self.ping_max_seq))
		case <-timercheckack.C:
			//fmt.Println("timercheckack")
		case <-timerack.C:
			now = int64(time.Now().UnixNano() / int64(time.Millisecond))
			for i := self.recvData.lastok; i <= self.recvData.maxok; i++ {
				if self.recvData.header[i] == nil {
					break
				}
				self.recvData.lastok = i
			}
			timeout_seq := uint16(0)
			timeou_need := false
			if self.recvData.curack < self.recvData.lastok {
				head := &UdpHeader{}
				head.seq = self.recvData.lastok
				head.bitmask |= 1
				tmp, need := self.FillLastokAckHead(head)
				if tmp > 0 && need {
					timeout_seq = tmp
					timeou_need = true
				}
				self.sendAck(head)
				self.num_acklist++
				self.recvData.curack = self.recvData.lastok
				self.recvData.header[self.recvData.curack].seq = 0
			}
			if self.recvData.header[self.recvData.maxok] != nil {
				for i := self.recvData.curack; i <= self.recvData.maxok; i++ {
					if self.recvData.header[i] != nil {
						if now-self.recvData.header[i].time_recv >= 10 {
							timeout_seq = i
						}
						if self.recvData.header[i].seq != 0 {
							head := &UdpHeader{}
							head.seq = self.recvData.header[i].seq
							tmp, need := self.FillMaxokAckHead(head)
							if tmp > 0 && need {
								timeout_seq = tmp
								timeou_need = true
							}
							if !self.is_server {
								fmt.Println("发确认包", now, head.seq, head.bitmask, head.datasize)
							}
							self.sendAck(head)
							self.recvData.header[i].seq = 0
							self.num_ack++
						}
					} else {
						timeou_need = true
					}
				}
			}
			if timeout_seq > 0 && timeou_need && timeout_seq != self.timeout_seq {
				head := &UdpHeader{}
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
			head := &UdpHeader{}
			offset := head.Unserialize(b, all)
			if offset > 0 {
				//fmt.Println("ReadFromUDP: ", left, n, all, int(head.datasize))
				copy(b, b[offset:all])
				all -= offset
			} else {
				break
			}
			if head.seq != 0 && head.datasize > 0 && (head.bitmask&1 == 0) {
				if !self.is_server {
					fmt.Println("LoopRecvUDP: ", left, n, all, int(head.datasize))
				}
				self.recvDataCh <- head
			} else {
				if self.is_server {
					fmt.Println("LoopRecvUDP: ", left, n, all, head.seq, head.datasize, head.bitmask, head.GetAllSize())
				}
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
			head := &UdpHeader{}
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
				self.recvDataCh <- head
			} else {
				if self.is_server {
					fmt.Println("LoopRecvTCP: ", left, n, all, head.seq, head.datasize, head.bitmask, head.GetAllSize())
				}
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
