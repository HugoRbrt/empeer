package impl

import (
	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"io"
	"math/rand"
	"regexp"
	"sort"
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
		chunksHexs := strings.Split(string(fileContent), peer.MetafileSep)
		var chunkList [][]byte
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
	return false
}

// shareSearch propagate the search and get listen of ID to listen for response is it's a source
func (n *node) shareSearch(budget uint, msg types.SearchRequestMessage, except []string, Src bool) ([]string, error) {
	neighborsList := n.table.GetListNeighbors(except)
	var listRequestID []string
	nbNeighbors := uint(len(neighborsList))
	if nbNeighbors == 0 {
		return nil, nil
	}
	if budget <= nbNeighbors {
		for _, neighbor := range neighborsList[:budget] {
			// send to neighbor the search with budget == 1
			msg.Budget = 1
			if Src {
				msg.RequestID = xid.New().String()
				n.fileNotif.requestNotif(msg.RequestID, budget)
				listRequestID = append(listRequestID, msg.RequestID)
			}
			// send msg
			err := n.sendSearch(neighbor, msg)
			if err != nil {
				return nil, err
			}
		}
	} else {
		budgetPerNeighbor := budget / nbNeighbors
		if budget%nbNeighbors >= nbNeighbors-1 {
			budgetPerNeighbor++
		}
		for numNeighbor, neighbor := range neighborsList {
			neighborBudget := budgetPerNeighbor
			if numNeighbor == 0 {
				neighborBudget += budget - nbNeighbors*budgetPerNeighbor
			}
			// send to neighbor the search with budget neighborBudget
			msg.Budget = neighborBudget
			if Src {
				msg.RequestID = xid.New().String()
				n.fileNotif.requestNotif(msg.RequestID, budget)
				listRequestID = append(listRequestID, msg.RequestID)
			}
			err := n.sendSearch(neighbor, msg)
			if err != nil {
				return nil, err
			}
		}
	}
	return listRequestID, nil
}

// sendSearch send the search message to neighbor
func (n *node) sendSearch(neighbor string, msg types.SearchRequestMessage) (err error) {
	hdr := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)

	transMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}
	pkt := transport.Packet{Header: &hdr, Msg: &transMsg}
	return n.conf.Socket.Send(neighbor, pkt, time.Millisecond*1000)
}

// FullyKnownFile return the firs file fully known by a peer, or "" if it doesn't exist
func (n *node) FullyKnownFile(listID []string) string {
	for _, id := range listID {
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
