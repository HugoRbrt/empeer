package udp

import (
	"errors"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{
		incomings: make(map[string]net.UDPConn),
	}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	sync.RWMutex
	// map between addresses and established UDP connections
	incomings map[string]net.UDPConn
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	n.Lock()
	defer n.Unlock()
	udpAdr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return &Socket{}, err
	}
	udpCon, err := net.ListenUDP("udp4", udpAdr)
	if err != nil {
		return &Socket{}, err
	}
	// add listening connection at our own address
	n.incomings[udpCon.LocalAddr().String()] = *udpCon

	return &Socket{
		UDP:    n,
		myAddr: udpCon.LocalAddr().String(),
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	myAddr string
	//mutex controlling access of ins packets
	muIns sync.RWMutex
	ins   []transport.Packet
	//mutex controlling access of outs packets
	muOuts sync.RWMutex
	outs   []transport.Packet
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	s.Lock()
	defer s.Unlock()
	udpConn := s.incomings[s.myAddr]
	err := udpConn.Close()
	if err != nil {
		return err
	}
	delete(s.incomings, s.myAddr)

	return nil
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {

	udpAdr, err := net.ResolveUDPAddr("udp4", dest)
	conn, err := net.DialUDP("udp4", nil, udpAdr)
	if err != nil {
		return err
	}

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	data, err := pkt.Marshal()
	if err != nil {
		return err
	}

	err = conn.SetWriteDeadline(time.Now().Add(timeout))
	_, errSd := conn.Write(data)
	if errSd != nil {
		return errSd
	}

	s.muOuts.Lock()
	defer s.muOuts.Unlock()
	s.outs = append(s.outs, pkt.Copy())

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {

	conn := s.incomings[s.myAddr]

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buffer := make([]byte, bufSize)
	readlen, _, errRcv := conn.ReadFromUDP(buffer)
	if errors.Is(errRcv, os.ErrDeadlineExceeded) {
		return transport.Packet{}, transport.TimeoutError(0)
	}
	if errRcv != nil {
		return transport.Packet{}, errRcv
	}

	var packet transport.Packet
	err := packet.Unmarshal(buffer[:readlen])
	if err != nil {
		return transport.Packet{}, err
	}

	s.muIns.Lock()
	defer s.muIns.Unlock()
	s.ins = append(s.ins, packet.Copy())

	return packet, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.myAddr
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	s.muIns.RLock()
	defer s.muIns.RUnlock()

	res := make([]transport.Packet, len(s.ins))

	for i, pkt := range s.ins {
		res[i] = pkt.Copy()
	}

	return res
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	s.muOuts.RLock()
	defer s.muOuts.RUnlock()

	res := make([]transport.Packet, len(s.outs))

	for i, pkt := range s.ins {
		res[i] = pkt.Copy()
	}

	return res
}
