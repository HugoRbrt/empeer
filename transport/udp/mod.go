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

const bufferSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (u *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	udpAdr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return &Socket{}, err
	}
	udpCon, err := net.ListenUDP("udp4", udpAdr)
	if err != nil {
		return &Socket{}, err
	}

	return &Socket{
		conn: *udpCon,
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	// state of established udp connection
	conn net.UDPConn
	// list of incoming packets
	ins packets
	// list of outgoing packets
	outs packets
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	err := s.conn.Close()
	return err
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {

	udpAdr, err := net.ResolveUDPAddr("udp4", dest)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp4", nil, udpAdr)
	if err != nil {
		return err
	}

	if timeout <= 0 {
		timeout = math.MaxInt64
	}
	err = conn.SetWriteDeadline(time.Now().Add(timeout))
	if err != nil {
		return err
	}

	data, err := pkt.Marshal()
	if err != nil {
		return err
	}

	_, errSd := conn.Write(data)
	if errSd != nil {
		return errSd
	}

	s.outs.Add(pkt.Copy())
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {

	if timeout > 0 {
		err := s.conn.SetReadDeadline(time.Now().Add(timeout))
		if err != nil {
			return transport.Packet{}, err
		}
	}

	buffer := make([]byte, bufferSize)
	readlen, _, errRcv := s.conn.ReadFromUDP(buffer)

	if errors.Is(errRcv, os.ErrDeadlineExceeded) {
		return transport.Packet{}, transport.TimeoutError(0)
	} else if errRcv != nil {
		return transport.Packet{}, errRcv
	}

	var packet transport.Packet
	err := packet.Unmarshal(buffer[:readlen])
	if err != nil {
		return transport.Packet{}, err
	}

	s.ins.Add(packet.Copy())

	return packet, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.conn.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.GetCopy()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.GetCopy()
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type packets struct {
	mu   sync.RWMutex
	list []transport.Packet
}

// Add packet to a packets list
func (l *packets) Add(pkt transport.Packet) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.list = append(l.list, pkt)
}

// GetCopy return a packet’s list copy
func (l *packets) GetCopy() []transport.Packet {
	l.mu.RLock()
	defer l.mu.RUnlock()

	res := make([]transport.Packet, len(l.list))

	for i, pkt := range l.list {
		res[i] = pkt.Copy()
	}

	return res
}
