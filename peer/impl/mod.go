package impl

import (
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// save the configuration of the node
	node := node{conf: conf}
	// Initializing the routing table
	node.table = ConcurrentRouteTable{R: make(map[string]string)}
	node.rumors.Init()
	node.table.SetEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
	return &node
}

func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Info().Msg(chatMsg.String())

	return nil
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// Send back an AckMessage to the source
	// TODO: fill the status message
	statusMsg := types.StatusMessage{}
	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        statusMsg,
	}
	transAck, err := n.conf.MessageRegistry.MarshalMessage(&ack)
	ackHeader := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	ackPacket := transport.Packet{Header: &ackHeader, Msg: &transAck}
	if err != nil {
		return err
	}
	err = n.conf.Socket.Send(pkt.Header.Source, ackPacket, time.Millisecond*10)
	if err != nil {
		return err
	}
	noExpected := true // value to verify if at least one rumor is expected
	// Process each expected rumor contained in the RumorsMessage
	for _, rumor := range rumorsMsg.Rumors {
		// we ignore not expected rumors
		if !n.rumors.IsExpected(rumor.Origin, int(rumor.Sequence)) {
			log.Info().Msg("broadcast NOT EXPECTED, origin:" + rumor.Origin + "seq:" + strconv.FormatUint(uint64(rumor.Sequence), 10))
			continue
		}
		noExpected = false
		// process rumor’s embedded message
		packet := transport.Packet{
			Header: pkt.Header,
			Msg:    rumor.Msg,
		}
		err := n.conf.MessageRegistry.ProcessPacket(packet)
		if err != nil {
			return err
		}
		n.rumors.Process(rumor.Origin, rumor)
	}

	// Send the RumorsMessage to another random neighbor if at least on rumor is expected
	if !noExpected {
		err, rdmNeighbor := n.table.GetRandomNeighbors([]string{n.conf.Socket.GetAddress()})
		if err != nil {
			return err
		}
		headerRelay := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), rdmNeighbor, 0)
		pktRelay := transport.Packet{
			Header: &headerRelay,
			Msg:    pkt.Msg,
		}
		return n.conf.Socket.Send(rdmNeighbor, pktRelay, time.Millisecond*10)
	}
	return nil
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
	table ConcurrentRouteTable
	// Broadcast functionality manager:
	rumors RumorsManager
}

// Start implements peer.Service
func (n *node) Start() error {
	// create a new context which allows goroutine to know if Stop() is call
	n.ctx, n.cancel = context.WithCancel(context.Background())
	channelError := make(chan error, 1)
	// add handler associated to all known message types
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)

	// we signal when the goroutine starts and when it ends
	n.wg.Add(1)
	go func(c chan error) {
		defer n.wg.Done()
		//start anti-entropy system
		c <- n.AntiEntropy()
	}(channelError)

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
	// is this message for this node?
	if pkt.Header.Destination == n.conf.Socket.GetAddress() {
		// yes: register it
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Info().Msgf("Error: %v", err.Error())
			return err
		}
	} else {
		// no: relay the message to the next hop if it exists
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
	//block until all goroutines are done
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
	return n.conf.Socket.Send(nextHop, packet, time.Millisecond*10)
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	// pick a random neighbor
	err, neighbor := n.table.GetRandomNeighbors([]string{n.conf.Socket.GetAddress()})
	if err != nil {
		return err
	}
	// create RumorsMessage containing one Rumor (embeds msg)
	rumor := types.Rumor{
		Origin:   n.conf.Socket.GetAddress(),
		Sequence: n.rumors.GetSeq(),
		Msg:      &msg,
	}
	rumors := types.RumorsMessage{
		Rumors: []types.Rumor{
			rumor,
		},
	}
	n.rumors.IncSeq()
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(rumors)
	if err != nil {
		return err
	}
	hdrRelay := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
	pkToRelay := transport.Packet{Header: &hdrRelay, Msg: &transMsg}
	if err != nil {
		return err
	}
	//send RumorsMessage to a random neighbor
	err = n.conf.Socket.Send(neighbor, pkToRelay, time.Millisecond*10)
	if err != nil {
		return err
	}
	log.Info().Msg("DONE")

	// Process the message locally
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	err = n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		return err
	}
	n.rumors.Process(n.conf.Socket.GetAddress(), rumor)

	//TODO: wait the Ack Msg
	return err
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

func (n *node) AntiEntropy() error {
	// TODO: remove this line (just for gui testing)
	n.conf.AntiEntropyInterval = time.Duration(10000000000)
	if n.conf.AntiEntropyInterval == 0 {
		// if interval of 0 is given then the anti-entropy mechanism must not be activated
		return nil
	}
	for {
		// check if Stop was called (and stop goroutine if so)
		select {
		case <-n.ctx.Done():
			return nil
		default:
		}
		time.Sleep(n.conf.AntiEntropyInterval)
		err := n.SendView()
		if err != nil {
			return err
		}
	}
}

func (n *node) SendView() error {
	//TODO: send anything if  the view is empty (e.g. if the node not already receive any rumor)
	err, neighbor := n.table.GetRandomNeighbors([]string{n.conf.Socket.GetAddress()})
	if err != nil {
		// if no neighbor was found: do nothing
		return nil
	}
	statusMsg := types.StatusMessage(n.rumors.GetView())
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(&statusMsg)
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
	pkt := transport.Packet{Header: &header, Msg: &transMsg}
	return n.conf.Socket.Send(neighbor, pkt, time.Millisecond*10)
}

// ConcurrentRouteTable define a safe way to access the RoutingTable.
type ConcurrentRouteTable struct {
	R  peer.RoutingTable
	mu sync.Mutex
}

func (sr *ConcurrentRouteTable) String() string {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.R.String()
}

// DisplayGraph displays the routing table as a graphviz graph.
func (sr *ConcurrentRouteTable) DisplayGraph(out io.Writer) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.R.DisplayGraph(out)
}

// SetEntry set a routing entry and override it if the entry already exist
func (sr *ConcurrentRouteTable) SetEntry(key, str string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.R[key] = str
}

// DeleteEntry delete a routing entry or do nothing if the entry doesn't exist
func (sr *ConcurrentRouteTable) DeleteEntry(key string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.R, key)
}

// Copy return a copy of the RoutingTable
func (sr *ConcurrentRouteTable) Copy() peer.RoutingTable {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	tableCopy := make(map[string]string)
	for key, value := range sr.R {
		tableCopy[key] = value
	}
	return tableCopy
}

// Get return the corresponding value and boolean corresponding to the existence of the value
func (sr *ConcurrentRouteTable) Get(key string) (string, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	value, b := sr.R[key]
	return value, b
}

// GetNeighbors return the list of node's neighbors
func (sr *ConcurrentRouteTable) GetNeighbors() []string {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	var b []string
	for key, element := range sr.R {
		if key == element {
			b = append(b, key)
		}
	}
	return b
}

// GetRandomNeighbors return a random neighbor of the node which is node in except Array
func (sr *ConcurrentRouteTable) GetRandomNeighbors(except []string) (error, string) {
	allNeighbors := (*sr).GetNeighbors()
	sr.mu.Lock()
	defer sr.mu.Unlock()
	var neighborsExcept []string
	for _, n := range allNeighbors {
		remove := false
		for _, e := range except {
			if n == e {
				remove = true
			}
		}
		if !remove {
			neighborsExcept = append(neighborsExcept, n)
		}
	}
	if len(neighborsExcept) == 0 {
		return xerrors.Errorf("no random neighbors found"), ""
	}
	rdmNeighbor := neighborsExcept[rand.Int()%len(neighborsExcept)]
	log.Info().Msg("neighbor found:" + rdmNeighbor)
	return nil, rdmNeighbor
}

type RumorsManager struct {
	// map of all processed rumors from each peer
	rumorsHistory map[string][]types.Rumor
	mu            sync.RWMutex
	// Counter of Broadcast messages made by the node
	sequence int
}

// Init initialize processedRumors
func (pr *RumorsManager) Init() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	(*pr).rumorsHistory = make(map[string][]types.Rumor)
	(*pr).sequence = 1
}

// IsExpected return if a rumors is expected by a node
func (pr *RumorsManager) IsExpected(addr string, sequence int) bool {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return len((*pr).rumorsHistory[addr])+1 == sequence
}

// Process add un rumor in its history  to signal the rumor  is processed
func (pr *RumorsManager) Process(addr string, r types.Rumor) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	(*pr).rumorsHistory[addr] = append((*pr).rumorsHistory[addr], r)
}

// GetView return a view (all the rumors the node has processed so far)
func (pr *RumorsManager) GetView() map[string]uint {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	mapCopy := make(map[string]uint)
	for k, v := range (*pr).rumorsHistory {
		mapCopy[k] = uint(len(v))
	}
	return mapCopy
}

// IncSeq increment the sequence number (after sending a broadcast message)
func (pr *RumorsManager) IncSeq() {
	(*pr).sequence++
}

// GetSeq return the sequence number of the last broadcast made by this node
func (pr *RumorsManager) GetSeq() uint {
	return uint((*pr).sequence)
}
