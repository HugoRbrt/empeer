package impl

import (
	"context"
	"errors"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// save the configuration of the node
	node := node{conf: conf}
	// Initializing the routing table
	node.table = ConcurrentRoutTable{R: make(map[string]string)}
	node.table.SetEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
	//Set CallBack messages for every type of messages
	node.SetMessageCallback()

	return &node
}

func (n *node) SetMessageCallback() {
	//set CallBack fot Chat Message
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, func(l *zerolog.Logger) registry.Exec {
		return func(m types.Message, p transport.Packet) error {
			l.With().
				Str("packet_id", p.Header.PacketID).
				Str("message_type", p.Msg.Type).
				Str("source", p.Header.Source).
				Logger()
			return nil
		}
	}(&n.logger))
	//set Callback for others... (in following homeworks)
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// keep the peer.Configuration:
	conf peer.Configuration

	// boolean about the running state of the node
	ctx    context.Context
	cancel context.CancelFunc
	//group of all goroutine launch by the node
	wg sync.WaitGroup
	//route table
	table ConcurrentRoutTable
	//log received messages
	logger zerolog.Logger
}

// Start implements peer.Service
func (n *node) Start() error {
	// create a new context which allows goroutine to know if Stop() is call
	n.ctx, n.cancel = context.WithCancel(context.Background())
	channelError := make(chan error, 1)
	// we signal when the goroutine starts and when it ends
	n.wg.Add(1)

	go func(c chan error, ctx context.Context) {
		defer n.wg.Done()
		for {
			// check if Stop was called (and stop goroutine if so)
			select {
			case <-ctx.Done():
				return
			default:
			}

			pkt, err := n.conf.Socket.Recv(time.Millisecond * 10)
			if errors.Is(err, transport.TimeoutError(0)) {
				continue
			} else if err != nil {
				c <- err
			}
			n.wg.Add(1)
			go func(c chan error) {
				defer n.wg.Done()
				err := n.ProcessMessage(pkt)
				if err != nil {
					c <- err
				}
			}(channelError)
		}
	}(channelError, n.ctx)

	select {
	case returnError := <-channelError:
		return returnError
	default:
		return nil
	}
}

// ProcessMessage permit to process a message (relay, register...)
func (n *node) ProcessMessage(pkt transport.Packet) error {
	// message registration
	_ = n.conf.MessageRegistry.ProcessPacket(pkt)
	// relay the message to the next hop if the node is not it's destination
	if pkt.Header.Destination != n.conf.Socket.GetAddress() {
		header := transport.NewHeader(pkt.Header.Source, n.conf.Socket.GetAddress(), pkt.Header.Destination, 0)
		packet := transport.Packet{Header: &header, Msg: pkt.Msg}
		nextHop, exist := n.table.Get(pkt.Header.Destination)
		if !exist {
			return xerrors.Errorf("unknown destination address")
		}
		return n.conf.Socket.Send(nextHop, packet, time.Millisecond*10)
	}
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	// warn all goroutine to stop
	n.cancel()
	//block until all goroutine are done
	n.wg.Wait()
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	nextHop, exist := n.table.Get(dest)
	if !exist {
		return xerrors.Errorf("unknown destination address")
	}
	return n.conf.Socket.Send(nextHop, packet, time.Second*1)
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

// ConcurrentRoutTable define a safe way to access the RoutingTable.
type ConcurrentRoutTable struct {
	R  peer.RoutingTable
	mu sync.Mutex
}

func (sr *ConcurrentRoutTable) String() string {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.R.String()
}

// DisplayGraph displays the routing table as a graphviz graph.
func (sr *ConcurrentRoutTable) DisplayGraph(out io.Writer) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.R.DisplayGraph(out)
}

// SetEntry set a routing entry and override it if the entry already exist
func (sr *ConcurrentRoutTable) SetEntry(key, str string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.R[key] = str
}

// DeleteEntry delete a routing entry or do nothing if the entry doesn't exist
func (sr *ConcurrentRoutTable) DeleteEntry(key string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.R, key)
}

// Copy return a copy of the RoutingTable
func (sr *ConcurrentRoutTable) Copy() peer.RoutingTable {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	tableCopy := make(map[string]string)
	for key, value := range sr.R {
		tableCopy[key] = value
	}
	return tableCopy
}

// Get return the corresponding value and boolean corresponding to the existence of the value
func (sr *ConcurrentRoutTable) Get(key string) (string, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	value, b := sr.R[key]
	return value, b
}
