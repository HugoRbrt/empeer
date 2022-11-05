package impl

import (
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"io"
	"math/rand"
	"sync"
)

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
	if c.catalog[key] == nil {
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
