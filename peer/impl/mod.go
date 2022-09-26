package impl

import (
	"errors"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// save the configuration of the node
	node := node{conf: conf}
	// Initializing the routing table
	node.table = peer.SafeRoutingTable{R: make(map[string]string)}
	node.table.SetEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())

	return &node
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// boolean about the running state of the node
	isRunning bool
	// keep the peer.Configuration:
	conf peer.Configuration
	//group of all goroutine launch by the node
	wg sync.WaitGroup
	//route table
	table peer.SafeRoutingTable
}

// Start implements peer.Service
func (n *node) Start() error {
	n.isRunning = true
	channelError := make(chan error, 1)
	go func(c chan error) {
		// we signal when the goroutine starts and when it ends
		n.wg.Add(1)
		defer n.wg.Add(-1)

		for {
			if !n.isRunning { // loop should exit once the Stop function is called
				c <- nil
				break
			}
			pkt, err := n.conf.Socket.Recv(time.Second * 1)
			if errors.Is(err, transport.TimeoutError(0)) {
				continue
			}
			if err != nil {
				c <- err
			}

			err = n.conf.MessageRegistry.ProcessPacket(pkt)
			if err != nil {
				c <- err
			}
		}
	}(channelError)
	select {
	case returnError := <-channelError:
		return returnError
	default:
		return nil
	}
}

// Stop implements peer.Service
func (n *node) Stop() error {
	// must block until all goroutines are done.
	n.isRunning = false
	n.wg.Wait()
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.GetRoutingTable()[dest], dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	return n.conf.Socket.Send(dest, packet, time.Second*1)
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addresses ...string) {
	for _, addr := range addresses {
		if n.conf.Socket.GetAddress() == addr { // Adding ourself should have no effect.
			continue
		}
		n.table.SetEntry(addr, addr)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.table.Copy()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.table.DeleteEntry(origin)
	} else {
		n.table.SetEntry(origin, relayAddr)
	}
}
