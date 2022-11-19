package impl

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConcurrentRouteTable

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

// GetListNeighbors return all neighbors of the peer in a random order
func (sr *ConcurrentRouteTable) GetListNeighbors(except []string) (list []string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	for _, neighbor := range sr.R {
		remove := false
		for _, e := range except {
			if neighbor == e {
				remove = true
			}
		}
		if !remove {
			list = append(list, neighbor)
		}
	}
	rand.Shuffle(len(list), func(i, j int) { list[i], list[j] = list[j], list[i] })
	return removeDuplicateValues(list)
}

// RumorsManager

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
func (n *node) CompareView(me map[string]uint, other map[string]uint) []string {
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

// Notification

// Notification notify which ack has been received by whom
type Notification struct {
	// notif create for each PacketID which need an ack a channel for signaling if ack has been received or not
	notif map[string]chan []byte
	mu    sync.RWMutex
}

// Init initialize Notification
func (an *Notification) Init() {
	an.mu.Lock()
	defer an.mu.Unlock()
	an.notif = make(map[string]chan []byte)
}

// requestNotif request for an Ack with the ID pckID
func (an *Notification) requestNotif(pckID string) {
	an.mu.Lock()
	defer an.mu.Unlock()
	an.notif[pckID] = make(chan []byte, 1)
}

// waitNotif return a channel which is closed when ack has been received
func (an *Notification) waitNotif(pckID string) chan []byte {
	an.mu.Lock()
	defer an.mu.Unlock()
	channel := an.notif[pckID]
	return channel
}

// signalNotif signal by its corresponding channel that the pckID's Ack was received and its content
func (an *Notification) signalNotif(pckID string, value []byte) {
	an.mu.Lock()
	channel := an.notif[pckID]
	an.mu.Unlock()
	channel <- value
}

// FilesNotification

// FilesNotification notify files obtained after a search
type FilesNotification struct {
	// notif create for each PacketID which need an ack a channel for signaling if ack has been received or not
	notif map[string]chan []types.FileInfo
	mu    sync.RWMutex
}

// Init initialize FilesNotification
func (fan *FilesNotification) Init() {
	fan.mu.Lock()
	defer fan.mu.Unlock()
	fan.notif = make(map[string]chan []types.FileInfo)
}

// requestNotif request for a reply with id pckID
func (fan *FilesNotification) requestNotif(pckID string, size uint) {
	fan.mu.Lock()
	defer fan.mu.Unlock()
	fan.notif[pckID] = make(chan []types.FileInfo, size)
}

// waitNotif return a channel which contained responses with pckID
func (fan *FilesNotification) waitNotif(pckID string) chan []types.FileInfo {
	fan.mu.Lock()
	defer fan.mu.Unlock()
	channel := fan.notif[pckID]
	return channel
}

// signalNotif close the corresponding request notification
func (fan *FilesNotification) signalNotif(pckID string) {
	fan.mu.Lock()
	defer fan.mu.Unlock()
	close(fan.notif[pckID])
}

// sendNotif signal by its corresponding channel that the pckID's Ack was received and its content
func (fan *FilesNotification) sendNotif(pckID string, value []types.FileInfo) {
	fan.mu.Lock()
	channel := fan.notif[pckID]
	channel <- value
	fan.mu.Unlock()
}

// ConcurrentCatalog

// ConcurrentCatalog define a safe way to access the Catalog.
type ConcurrentCatalog struct {
	catalog peer.Catalog
	mu      sync.RWMutex
}

// Init initialize the concurrentCatalog
func (c *ConcurrentCatalog) Init() {
	c.mu.Lock()
	defer c.mu.Unlock()
	(*c).catalog = make(peer.Catalog)
}

// GetCatalog returns the peer's catalog.
func (c *ConcurrentCatalog) GetCatalog() peer.Catalog {
	c.mu.Lock()
	defer c.mu.Unlock()
	catalogCopy := make(peer.Catalog)
	for k, v := range (*c).catalog {
		tempMap := make(map[string]struct{})
		for k2, v2 := range v {
			tempMap[k2] = v2
		}
		catalogCopy[k] = tempMap
	}
	return catalogCopy
}

// UpdateCatalog tells the peer about a piece of data referenced by 'key'
// being available on peer
func (c *ConcurrentCatalog) UpdateCatalog(key string, peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.catalog[key]
	if !ok {
		c.catalog[key] = make(map[string]struct{})
	}

	c.catalog[key][peer] = struct{}{}
}

// RandomPeer return a random peer containing the chunk defined by its key or "" if there's no peer
func (c *ConcurrentCatalog) RandomPeer(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.catalog[key] == nil {
		return ""
	}
	peers := make([]string, 0, len(c.catalog[key]))
	for p := range c.catalog[key] {
		peers = append(peers, p)
	}
	return peers[rand.Int()%len(peers)]
}

// OTHER FUNCTIONS

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
	diff := n.CompareView(msg, n.rumors.GetView())
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
	n.waitAck.requestNotif(pkToRelay.Header.PacketID)
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

// removeDuplicateValues return strSlice without duplicate values
func removeDuplicateValues(strSlice []string) []string {
	var list []string
	for _, val := range strSlice {
		isPresent := false
		for _, val2 := range list {
			if val == val2 {
				isPresent = true
				break
			}
		}
		if !isPresent {
			list = append(list, val)
		}
	}
	return list
}

// searchLocally return list of matching local files
func (n *node) searchLocally(reg regexp.Regexp, WithEmpty bool) (files []types.FileInfo) {
	files = []types.FileInfo{}
	storageSpace := n.conf.Storage.GetDataBlobStore()
	n.conf.Storage.GetNamingStore().ForEach(
		func(name string, hashFile []byte) bool {
			if reg.Match([]byte(name)) {
				file := types.FileInfo{Name: name, Metahash: string(hashFile)}
				if parseAndStoreFile(file, WithEmpty, &files, storageSpace) {
					return true
				}
			}
			return true
		},
	)
	return files
}

// parseAndStoreFile update files by adding FileInfo of the corresponding file
func parseAndStoreFile(file types.FileInfo, WithEmpty bool, files *[]types.FileInfo, storageSpace storage.Store) bool {
	name := file.Name
	hashFile := file.Metahash
	fileContent := storageSpace.Get(hashFile)
	if fileContent == nil {
		if WithEmpty {
			*files = append(*files, types.FileInfo{
				Name:     name,
				Metahash: string(fileContent),
				Chunks:   nil,
			})
		} else {
			return true
		}

	} else {
		ParseAndStoreChunks(fileContent, storageSpace, files, name, hashFile)
	}
	return false
}

// ParseAndStoreChunks update files by adding FileInfo of the corresponding chunks
func ParseAndStoreChunks(fileContent []byte, storageSpace storage.Store,
	files *[]types.FileInfo, name string, hashFile string) {
	var chunkList [][]byte
	chunksHexs := strings.Split(string(fileContent), peer.MetafileSep)
	for _, chunkHex := range chunksHexs {
		c := storageSpace.Get(chunkHex)
		if c == nil {
			chunkList = append(chunkList, nil)
		} else {
			chunkList = append(chunkList, []byte(chunkHex))
		}
	}
	*files = append(*files, types.FileInfo{
		Name:     name,
		Metahash: hashFile,
		Chunks:   chunkList,
	})
}

// shareSearch propagate the search and get listen of ID to listen for response is it's a source
func (n *node) shareSearch(budget uint, msg types.SearchRequestMessage, except []string, Src bool) ([]string, error) {
	neighborsList := n.table.GetListNeighbors(except)
	var listRequestID []string
	var budgets []uint
	nbNeighbors := uint(len(neighborsList))
	if nbNeighbors == 0 {
		return nil, nil
	}
	// distribute budget between neighbors
	if budget <= nbNeighbors {
		budgets = make([]uint, budget)
		for i := range budgets {
			budgets[i] = 1
		}
	} else {
		budgets = make([]uint, len(neighborsList))
		budgetPerNeighbor := budget / nbNeighbors
		if budget%nbNeighbors >= nbNeighbors-1 {
			budgetPerNeighbor++
		}
		for numNeighbor := range neighborsList {
			neighborBudget := budgetPerNeighbor
			if numNeighbor == 0 {
				neighborBudget += budget - nbNeighbors*budgetPerNeighbor
			}
			// send to neighbor the search with budget neighborBudget
			budgets[numNeighbor] = neighborBudget
		}
	}
	err := n.DistributeSearch(budgets, msg, Src, &listRequestID, neighborsList)
	if err != nil {
		return nil, err
	}
	return listRequestID, nil
}

// DistributeSearch send the search message to neighbors with respective budget
func (n *node) DistributeSearch(budgets []uint, msg types.SearchRequestMessage, Src bool,
	listRequestID *[]string, neighbors []string) error {
	for i, budget := range budgets {
		msg.Budget = budget
		if Src {
			msg.RequestID = xid.New().String()
			n.fileNotif.requestNotif(msg.RequestID, budget)
			*listRequestID = append(*listRequestID, msg.RequestID)
		}
		hdr := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbors[i], 0)
		transMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
		if err != nil {
			return err
		}
		pkt := transport.Packet{Header: &hdr, Msg: &transMsg}

		err = n.conf.Socket.Send(neighbors[i], pkt, time.Millisecond*1000)
		if err != nil {
			return err
		}
	}
	return nil
}

// FullyKnownFile return the first file fully known (or "" if not) obtained by listening iin responsesId
func (n *node) FullyKnownFile(responsesID []string) string {
	for _, id := range responsesID {
		channelIsClosed := false
		for !channelIsClosed {
			select {
			case responses := <-n.fileNotif.waitNotif(id):
				result := ContainFullyKnown(responses)
				if result != "" {
					return result
				}
			default:
				n.fileNotif.signalNotif(id)
				channelIsClosed = true
			}
		}
	}
	return ""
}

// ContainFullyKnown return the first file fully known in a list, "" if it doesn't exist
func ContainFullyKnown(files []types.FileInfo) string {
	for _, file := range files {
		fullyKnown := true
		for _, chunk := range file.Chunks {
			if chunk == nil {
				fullyKnown = false
			}
		}
		if fullyKnown {
			return file.Name
		}
	}
	return ""
}

// Acceptor

// Acceptor define a node with the acceptor role in Paxos
type Acceptor struct {
	*node

	maxId         uint
	step          uint
	acceptedID    uint
	acceptedValue *types.PaxosValue
}

// NewAcceptor permit to create an acceptor role for the node at step s
func (n *node) NewAcceptor(s uint) (a *Acceptor) {
	return &Acceptor{
		node:          n,
		maxId:         0,
		step:          s,
		acceptedValue: nil,
		acceptedID:    0,
	}
}

// Proposer

// Proposer define a node with the proposer role in Paxos
type Proposer struct {
	*node

	step          uint
	nbResponses   uint
	id            uint
	maxAcceptedId uint
	acceptedValue *types.PaxosValue
	proposedValue *types.PaxosValue
	phase         uint
	mu            sync.Mutex
}

// NewProposer permit to create an acceptor role for the node at step s
func (n *node) NewProposer(s uint) (a *Proposer) {
	return &Proposer{
		node:          n,
		step:          s,
		nbResponses:   0,
		maxAcceptedId: 0,
		acceptedValue: nil,
		proposedValue: nil,
		phase:         1,
		id:            n.conf.PaxosID,
	}
}

func (p *Proposer) ProposeConsensus(proposedValue types.PaxosValue) (*types.PaxosValue, error) {
	for {
		// begin phase 1
		err := p.SendPrepare()
		if err != nil {
			return nil, err
		}
		select {
		case <-p.ctx.Done():
			return nil, nil
		default:
		}
		//begin Phase 2
		v := &proposedValue
		p.mu.Lock()
		p.proposedValue = &proposedValue
		if p.acceptedValue != nil {
			v = p.acceptedValue
		}
		p.mu.Unlock()
		result, err := p.SendPropose(v)
		if err != nil {
			return nil, err
		}
		if result {
			// consensus is reached!
			log.Info().Msgf("consensus is reached")
			return v, nil
		}
		log.Info().Msgf("consensus not reached")
		// prepare message to retry sending Prepare message
		p.mu.Lock()
		p.id += p.conf.TotalPeers
		p.mu.Unlock()
	}
}

// SendPrepare send Paxos Prepare message, and return if Proposer received enough Promises
func (p *Proposer) SendPrepare() error {
	p.phase = 1
	// Send prepare message while we don't have a majority of promises
	for {
		// create Prepare Msg
		prepareMsg := types.PaxosPrepareMessage{
			Step:   0,
			ID:     p.id,
			Source: p.conf.Socket.GetAddress(),
		}
		msg, err := p.conf.MessageRegistry.MarshalMessage(prepareMsg)
		if err != nil {
			return err
		}
		p.nbResponses = 0
		err = p.Broadcast(msg)
		if err != nil {
			return err
		}
		//observe if promises response represent a majority
		timeout := false
		for !timeout {
			p.mu.Lock()
			if int(p.nbResponses) >= p.conf.PaxosThreshold(p.conf.TotalPeers) {
				// go to next phase
				p.mu.Unlock()
				return nil
			}
			p.mu.Unlock()
			select {
			case <-p.ctx.Done():
				return nil
			case <-time.After(p.conf.PaxosProposerRetry):
				// Retry SendPrepare
				timeout = true
			default:
			}
		}
		// prepare message to retry sending Prepare message
		p.id += p.conf.TotalPeers
	}
}

// SendPropose send Paxos Prepare message, and return if Proposer received enough Promises
func (p *Proposer) SendPropose(value *types.PaxosValue) (bool, error) {
	p.mu.Lock()
	p.phase = 2
	p.nbResponses = 0
	// create Propose Msg
	proposeMsg := types.PaxosProposeMessage{
		Step:  0,
		ID:    p.id,
		Value: *value,
	}
	p.mu.Unlock()
	// Send propose message while we don't have a majority of accept
	msg, err := p.conf.MessageRegistry.MarshalMessage(proposeMsg)
	if err != nil {
		return false, err
	}
	go func() {
		err = p.Broadcast(msg)
		if err != nil {
			log.Error().Msgf("error to broadcast tlc message")
		}
	}()
	//observe if accept responses represent a majority
	for {
		p.mu.Lock()
		if int(p.nbResponses) >= p.conf.PaxosThreshold(p.conf.TotalPeers) {
			log.Info().Msgf("consensus is reached")
			// consensus is reached!
			p.mu.Unlock()
			return true, nil
		}
		p.mu.Unlock()
		select {
		case <-p.ctx.Done():
			return false, nil
		case <-time.After(p.conf.PaxosProposerRetry):
			// Retry SendPrepare
			return false, nil
		default:
		}
	}
}

// TLC

type TLC struct {
	*node
	p           *Proposer
	a           *Acceptor
	step        uint
	broadcasted bool
	mu          sync.Mutex
	Resp        map[uint]struct {
		nb    uint
		value types.BlockchainBlock
	}
}

func (n *node) NewTLC() (tlc *TLC) {
	return &TLC{
		node:        n,
		p:           n.NewProposer(0),
		a:           n.NewAcceptor(0),
		step:        0,
		broadcasted: false,
		Resp: make(map[uint]struct {
			nb    uint
			value types.BlockchainBlock
		}),
	}
}

func (tlc *TLC) NextStep() {
	log.Info().Msgf("%v: next step", tlc.conf.Socket.GetAddress())
	tlc.step++
	tlc.broadcasted = false
	tlc.a = tlc.NewAcceptor(tlc.step)
	tlc.p = tlc.NewProposer(tlc.step)
}

func (tlc *TLC) NewBlock(value *types.PaxosValue) (types.BlockchainBlock, error) {
	newBlock := types.BlockchainBlock{
		Index:    tlc.step,
		Hash:     nil,
		Value:    *value,
		PrevHash: tlc.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey),
	}
	if newBlock.PrevHash == nil {
		newBlock.PrevHash = make([]byte, 32)
	}
	h := sha256.New()
	h.Write([]byte(strconv.Itoa(int(newBlock.Index))))
	h.Write([]byte(newBlock.Value.UniqID))
	h.Write([]byte(newBlock.Value.Filename))
	h.Write([]byte(newBlock.Value.Metahash))
	h.Write(newBlock.PrevHash)
	newBlock.Hash = h.Sum(nil)
	return newBlock, nil
}

func (tlc *TLC) SendTLC(block types.BlockchainBlock) error {
	log.Info().Msgf("%v: Broadcast TLC", tlc.conf.Socket.GetAddress())
	// Broadcast TLC msg
	msg := types.TLCMessage{
		Step:  tlc.step,
		Block: block,
	}
	transpMsg, err := tlc.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}
	tlc.broadcasted = true
	go func() {
		err = tlc.Broadcast(transpMsg)
		if err != nil {
			log.Error().Msgf("error to broadcast tlc message")
		}
	}()
	return nil
}

// AddBlock add the block to its own blockchain and store fileName/MetaHash association
func (tlc *TLC) AddBlock(block types.BlockchainBlock) error {
	log.Info().Msgf("adding block: %v", block.Value.Filename)
	blockchain := tlc.conf.Storage.GetBlockchainStore()
	buf, err := block.Marshal()
	if err != nil {
		return err
	}
	blockchain.Set(hex.EncodeToString(block.Hash), buf)
	blockchain.Set(storage.LastBlockKey, block.Hash)
	tlc.conf.Storage.GetNamingStore().Set(block.Value.Filename, []byte(block.Value.Metahash))
	return nil
}

func (tlc *TLC) LaunchConsensus(value types.PaxosValue) error {
	oursName := value.Filename
	NamingStore := tlc.conf.Storage.GetNamingStore()
	for {
		if NamingStore.Get(oursName) != nil {
			return xerrors.Errorf("name already exists: %s", NamingStore.Get(oursName))
		}
		_, err := tlc.p.ProposeConsensus(value)
		if err != nil {
			return err
		}
		/*
			if chosenValue != nil {
				// consensus is reached!
				block, err := tlc.NewBlock(chosenValue)
				if err != nil {
					return err
				}
				err = tlc.SendTLC(block)
				if err != nil {
					return err
				}
			}
			if err != nil {
				return err
			}
		*/
		if value.Filename == oursName {
			return nil
		}
	}
}
