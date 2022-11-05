package impl

import (
	"context"
	"crypto"
	"encoding/hex"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// save the configuration of the node
	node := node{conf: conf}
	// initialize node's objects
	node.table = ConcurrentRouteTable{R: make(map[string]string)}
	node.rumors.Init()
	node.waitAck.Init()
	node.catalog.Init()
	node.table.SetEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
	// create a new context which allows goroutine to know if Stop() is call
	node.ctx, node.cancel = context.WithCancel(context.Background())
	return &node
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
	// group of all goroutine launch by the node
	wg sync.WaitGroup
	// route table
	table ConcurrentRouteTable
	// Broadcast functionality manager:
	rumors RumorsManager
	// chanel list that ackMessage uses to notify that corresponding packetID ack has been received
	waitAck AckNotification
	// mutex controlling send/recv rumors msg
	rumorMu sync.Mutex
	// catalog defining where metahashes and chunks can be found
	catalog ConcurrentCatalog
}

// Start implements peer.Service
func (n *node) Start() error {
	// add handler associated to all known message types
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.ExecEmptyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.ExecStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.ExecPrivateMessage)

	// we signal when the goroutine starts and when it ends
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		//start anti-entropy system
		err := n.AntiEntropy()
		if err != nil {
			log.Error().Msgf("error from antiEntropy: %v", err.Error())
		}
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		//start heartbeat mechanism
		err := n.Heartbeat()
		if err != nil {
			log.Error().Msgf("error from heartbeat: %v", err.Error())
		}
	}()

	n.wg.Add(1)
	go func(ctx context.Context) {
		defer n.wg.Done()
		for {
			// check if Stop was called (and stop goroutine if so)
			select {
			case <-ctx.Done():
				return
			default:
			}
			pkt, err := n.conf.Socket.Recv(time.Millisecond * 1000)
			if errors.Is(err, transport.TimeoutError(0)) {
				continue
			} else if err != nil {
				log.Error().Msgf("error while receiving message: %v", err.Error())
			}
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				err := n.ProcessMessage(pkt)
				if err != nil {
					log.Error().Msgf("error while processing received message: %v", err.Error())
				}
			}()
		}
	}(n.ctx)
	return nil
}

// ProcessMessage permit to process a message (relay, register...)
func (n *node) ProcessMessage(pkt transport.Packet) error {
	// is this message for this node?
	if pkt.Header.Destination == n.conf.Socket.GetAddress() {
		// yes: register it
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
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
		return n.conf.Socket.Send(nextHop, packet, time.Millisecond*1000)
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
	return n.conf.Socket.Send(nextHop, packet, time.Millisecond*1000)
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
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
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(rumors)
	if err != nil {
		return err
	}

	// Process the message locally
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	err = n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		return err
	}
	n.rumors.Process(n.conf.Socket.GetAddress(), rumor, &n.table, "")

	// send a rumors to a random neighbor
	n.wg.Add(1)
	neighborAlreadyTry := ""
	go func() {
		defer n.wg.Done()
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		err2 := n.TryBroadcast(neighborAlreadyTry, transMsg)
		if err2 != nil {
			log.Error().Msg("Error while trying to broadcast a message")
		}
	}()
	return err
}

// TryBroadcast send a rumors to a random neighbor until someone receive it (for Broadcast function)
func (n *node) TryBroadcast(neighborAlreadyTry string, transMsg transport.Message) error {
	for {
		// pick a random neighbor
		ok, neighbor := n.table.GetRandomNeighbors([]string{neighborAlreadyTry, n.conf.Socket.GetAddress()})
		if !ok {
			// if no neighbor: send anything
			return nil
		}
		if neighborAlreadyTry == "" { // only if the broadcast is sent for the first time
			n.rumors.IncSeq()
		}
		hdrRelay := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
		pkToRelay := transport.Packet{Header: &hdrRelay, Msg: &transMsg}
		//send RumorsMessage to a random neighbor
		n.waitAck.requestAck(pkToRelay.Header.PacketID)
		err := n.conf.Socket.Send(neighbor, pkToRelay, time.Millisecond*1000)
		if err != nil {
			return err
		}
		// wait the Ack Msg
		// if timeout set to 0: wait forever
		if n.conf.AckTimeout == 0 {
			select {
			case <-n.ctx.Done():
				return nil
			case <-n.waitAck.waitAck(pkToRelay.Header.PacketID):
				return nil
			}
		}
		select {
		case <-n.ctx.Done():
			return nil
		case <-n.waitAck.waitAck(pkToRelay.Header.PacketID):
			// if ack was received
			return nil
		case <-time.After(n.conf.AckTimeout): // resend the message to another neighbor
			neighborAlreadyTry = neighbor
			continue
		}
	}
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

// AntiEntropy implement the anti entropy mechanism to make nodes’ views consistent
func (n *node) AntiEntropy() error {
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
		err := n.SendView([]string{}, "")
		if err != nil {
			return err
		}
	}
}

// Heartbeat implement the heartbeat mechanism which makes peers announce themselves to every other peer
func (n *node) Heartbeat() error {
	if n.conf.HeartbeatInterval == 0 {
		// if interval of 0 is given then the Heartbeat mechanism must not be activated
		return nil
	}
	transEmptyMsg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
	if err != nil {
		return err
	}
	for {
		// check if Stop was called (and stop goroutine if so)
		select {
		case <-n.ctx.Done():
			return nil
		default:
		}
		err = n.Broadcast(transEmptyMsg)
		if err != nil {
			return err
		}
		time.Sleep(n.conf.HeartbeatInterval)
	}
}

// Upload implement the peer.DataSharing
func (n *node) Upload(data io.Reader) (metahash string, err error) {
	var HashHexs []string
	var HashShas []byte
	storageSpace := n.conf.Storage.GetDataBlobStore()
	for {
		// read the next chunk
		var chunk = make([]byte, n.conf.ChunkSize)
		length, err := data.Read(chunk)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return "", err
			}
		}
		// compute hash
		hash := crypto.SHA256.New()
		_, err = hash.Write(chunk[:length])
		if err != nil {
			return "", err
		}
		hashSha := hash.Sum(nil)
		hashHex := hex.EncodeToString(hashSha)
		// store the chunk
		storageSpace.Set(hashHex, chunk[:length])
		// keep in memory Hash
		HashShas = append(HashShas, hashSha...)
		HashHexs = append(HashHexs, hashHex)
	}
	// Store MetaFile
	hash := crypto.SHA256.New()
	_, err = hash.Write(HashShas)
	if err != nil {
		return "", err
	}
	hashSha := hash.Sum(nil)
	metahash = hex.EncodeToString(hashSha)
	metaFileContent := []byte(strings.Join(HashHexs, peer.MetafileSep))
	storageSpace.Set(metahash, metaFileContent)
	return metahash, nil
}

// Download implement the peer.DataSharing
func (n *node) Download(metahash string) ([]byte, error) {
	// Get MetaFile
	metaFile := string(n.conf.Storage.GetDataBlobStore().Get(metahash)[:])
	// Get list of chunk's hashHex
	chunks := strings.Split(metaFile, peer.MetafileSep)
	// Get Chunks 1 by 1
	for _, chunk := range chunks {
		// check if it's locally, do anything (the chunk is already uploaded)
		if n.conf.Storage.GetDataBlobStore().Get(chunk) != nil {
			continue
		}
		// check if it knows someone who has the chunk, if so: send DataRequestMessage (random peer, retry on it with (next hop if needed) by waiting with backoff strategy) and store it locally
		if peerChunk := n.catalog.RandomPeer(chunk); peerChunk == "" {
			return nil, xerrors.Errorf("no peer found for chunk %v", chunk)
		}
		// send DataRequestMessage

	}
	return nil, nil
}

// GetCatalog implement the peer.DataSharing
func (n *node) GetCatalog() peer.Catalog {
	return n.catalog.GetCatalog()
}

// UpdateCatalog implement the peer.DataSharing
func (n *node) UpdateCatalog(key string, peer string) {
	n.catalog.UpdateCatalog(key, peer)
}

// SendView send the nodes' view by a statusMessage to dest or
// if dest == "", send to a random neighbor (except those given in params)
func (n *node) SendView(except []string, dest string) error {
	neighbor := dest
	var ok bool
	if neighbor == "" {
		// Pick a random neighbor which is not in except
		allowsNeighbors := append([]string{n.conf.Socket.GetAddress()}, except...)
		ok, neighbor = n.table.GetRandomNeighbors(allowsNeighbors)
		if !ok {
			// if no neighbor was found: do nothing
			return nil
		}
	}
	statusMsg := types.StatusMessage(n.rumors.GetView())
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(&statusMsg)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
	pkt := transport.Packet{Header: &header, Msg: &transMsg}
	return n.conf.Socket.Send(neighbor, pkt, time.Millisecond*1000)
}

// sendDiffView send rumors which msg doesn't have (in order of increasing sequence number)
func (n *node) sendDiffView(msg types.StatusMessage, dest string) error {
	diff := CompareView(msg, n.rumors.GetView())
	var rumorList []types.Rumor
	for _, addr := range diff {
		// send all rumors from addr which is not already received by
		rumorList = append(rumorList, n.rumors.GetRumorsFrom(addr)[msg[addr]:]...)
	}
	// Sort rumorList by ascending sequence number
	sort.Slice(rumorList, func(i, j int) bool {
		return rumorList[i].Sequence < rumorList[j].Sequence
	})
	rumors := types.RumorsMessage{
		Rumors: rumorList,
	}
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(rumors)
	if err != nil {
		return err
	}
	hdrRelay := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkToRelay := transport.Packet{Header: &hdrRelay, Msg: &transMsg}
	n.waitAck.requestAck(pkToRelay.Header.PacketID)
	return n.conf.Socket.Send(dest, pkToRelay, time.Millisecond*1000)
}

// SameStatus return m1 == m2 for StatusMessage
func SameStatus(m1 types.StatusMessage, m2 types.StatusMessage) bool {
	for i := range m1 {
		if m1[i] != m2[i] {
			return false
		}
	}
	for i := range m2 {
		if m1[i] != m2[i] {
			return false
		}
	}
	return true
}
