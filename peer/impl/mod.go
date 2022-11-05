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
	"math/rand"
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
	node.table.SetEntry(node.conf.Socket.GetAddress(), node.conf.Socket.GetAddress())
	// create a new context which allows goroutine to know if Stop() is call
	node.ctx, node.cancel = context.WithCancel(context.Background())
	return &node
}

// Handler for each types of message

func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	_ = pkt

	log.Info().Msg(chatMsg.String())

	return nil
}

func (n *node) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	_, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	_ = pkt

	return nil
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	n.rumorMu.Lock()
	// cast the message to its actual type. You assume it is the right type.
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		n.rumorMu.Unlock()
		return xerrors.Errorf("wrong type: %T", msg)
	}

	noExpected := true // value to verify if at least one rumor is expected
	// Process each expected rumor contained in the RumorsMessage
	for _, rumor := range rumorsMsg.Rumors {
		// we ignore not expected rumors
		if !n.rumors.IsExpected(rumor.Origin, int(rumor.Sequence)) {
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
			n.rumorMu.Unlock()
			return err
		}
		n.rumors.Process(rumor.Origin, rumor, &n.table, pkt.Header.RelayedBy)
	}

	// Send back an AckMessage to the source
	statusMsg := types.StatusMessage(n.rumors.GetView())
	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        statusMsg,
	}
	transAck, err := n.conf.MessageRegistry.MarshalMessage(&ack)
	if err != nil {
		n.rumorMu.Unlock()
		return err
	}
	ackHeader := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	ackPacket := transport.Packet{Header: &ackHeader, Msg: &transAck}
	n.rumorMu.Unlock()
	err = n.conf.Socket.Send(pkt.Header.Source, ackPacket, time.Millisecond*1000)
	if err != nil {
		return err
	}

	// Send the RumorsMessage to another random neighbor if at least on rumor is expected
	if !noExpected {
		ok, rdmNeighbor := n.table.GetRandomNeighbors([]string{n.conf.Socket.GetAddress(), pkt.Header.Source})
		if !ok {
			// if no other neighbors, do nothing
			return nil
		}
		headerRelay := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), rdmNeighbor, 0)
		pktRelay := transport.Packet{
			Header: &headerRelay,
			Msg:    pkt.Msg,
		}
		n.waitAck.requestAck(pktRelay.Header.PacketID)
		return n.conf.Socket.Send(rdmNeighbor, pktRelay, time.Millisecond*1000)
	}
	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	n.rumorMu.Lock()
	defer n.rumorMu.Unlock()
	// cast the message to its actual type. You assume it is the right type.
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	if SameStatus(*statusMsg, n.rumors.GetView()) {
		// if Both peers have the same view.
		// then ContinueMongering with probability "n.conf.ContinueMongering"
		if rand.Float64() < n.conf.ContinueMongering || n.conf.ContinueMongering == 1 {
			return n.SendView([]string{pkt.Header.Source}, "")
		}
		return nil
	}
	iDontHave := CompareView(n.rumors.GetView(), *statusMsg)
	if len(iDontHave) > 0 {
		// has Rumors that the remote peer doesn't have.
		// then send a status message to the remote peer
		err := n.SendView([]string{}, pkt.Header.Source)
		if err != nil {
			return err
		}
	}
	itDoesntHave := CompareView(*statusMsg, n.rumors.GetView())
	if len(itDoesntHave) > 0 {
		// remote peer has Rumors that the peer doesn't have
		// then sent rumors that remote peer doesn't have (in order of increasing sequence number)
		err := n.sendDiffView(*statusMsg, pkt.Header.Source)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// stops the timer
	n.waitAck.signalAck(ackMsg.AckedPacketID)
	// process embeds message
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(ackMsg.Status)
	if err != nil {
		return err
	}
	packet := transport.Packet{
		Header: pkt.Header,
		Msg:    &transMsg,
	}
	return n.conf.MessageRegistry.ProcessPacket(packet)
}

func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	privMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// Process only if the peer’s socket address is in the list of recipients
	if _, b := privMsg.Recipients[n.conf.Socket.GetAddress()]; b {
		packet := transport.Packet{
			Header: pkt.Header,
			Msg:    privMsg.Msg,
		}
		return n.conf.MessageRegistry.ProcessPacket(packet)
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

// Get return the value and boolean (if the key exist or not) corresponding to the key value in routing table
func (sr *ConcurrentRouteTable) Get(key string) (string, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	value, b := sr.R[key]
	return value, b
}

// GetRandomNeighbors return a random neighbor of the node which is node in except Array
func (sr *ConcurrentRouteTable) GetRandomNeighbors(except []string) (bool, string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	var allNeighbors []string
	for key, element := range sr.R {
		if key == element {
			allNeighbors = append(allNeighbors, key)
		}
	}
	// remove those which are in except
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
	// chose a random neighbors between those who remain
	if len(neighborsExcept) == 0 {
		return false, ""
	}
	rdmNeighbor := neighborsExcept[rand.Int()%len(neighborsExcept)]
	return true, rdmNeighbor
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
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return len((*pr).rumorsHistory[addr])+1 == sequence
}

// Process add un rumor in its history  to signal the rumor  is processed
func (pr *RumorsManager) Process(addr string, r types.Rumor, sr *ConcurrentRouteTable, relayBy string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	(*pr).rumorsHistory[addr] = append((*pr).rumorsHistory[addr], r)
	// update routing table
	if relayBy != "" {
		sr.SetEntry(addr, relayBy)
	}
}

// GetView return a view (all the rumors the node has processed so far)
func (pr *RumorsManager) GetView() map[string]uint {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	mapCopy := make(map[string]uint)
	for k, v := range (*pr).rumorsHistory {
		mapCopy[k] = uint(len(v))
	}
	return mapCopy
}

// GetRumorsFrom return the list of rumors received from src
func (pr *RumorsManager) GetRumorsFrom(src string) []types.Rumor {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	arr := (*pr).rumorsHistory[src]
	return arr
}

// CompareView returns a list of addresses where 'me' has not received the latest rumor that 'other' has
func CompareView(me map[string]uint, other map[string]uint) []string {
	var diff []string
	for addr := range other {
		if me[addr] < other[addr] {
			diff = append(diff, addr)
		}
	}
	return diff
}

// IncSeq increment the sequence number (after sending a broadcast message)
func (pr *RumorsManager) IncSeq() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	(*pr).sequence++
}

// GetSeq return the sequence number of the last broadcast made by this node
func (pr *RumorsManager) GetSeq() uint {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return uint((*pr).sequence)
}

// AckNotification notify which ack has been received by whom
type AckNotification struct {
	// notif create for each PacketID which need an ack a channel for signaling if ack has been received or not
	notif map[string]chan bool
	mu    sync.RWMutex
}

// Init initialize AckNotification
func (an *AckNotification) Init() {
	an.mu.Lock()
	defer an.mu.Unlock()
	an.notif = make(map[string]chan bool)
}

// waitAck request for an Ack with the ID pckID
func (an *AckNotification) requestAck(pckID string) {
	an.mu.Lock()
	defer an.mu.Unlock()
	an.notif[pckID] = make(chan bool)
}

// waitAck return a channel which is closed when ack has been received
func (an *AckNotification) waitAck(pckID string) chan bool {
	an.mu.Lock()
	defer an.mu.Unlock()
	channel := an.notif[pckID]
	return channel
}

// signalAck signal by its corresponding channel that the pckID's Ack was received
func (an *AckNotification) signalAck(pckID string) {
	an.mu.Lock()
	defer an.mu.Unlock()
	close(an.notif[pckID])
}
