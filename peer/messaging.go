package peer

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

// Messaging defines the functions for the basic functionalities to exchange
// messages between peers.
type Messaging interface {
	// Unicast sends a packet to a given destination. If the destination is the
	// same as the node's address, then the message must still be sent to the
	// node via its socket. Use transport.NewHeader to build the packet's
	// header.
	//
	// - implemented in HW0
	Unicast(dest string, msg transport.Message) error

	// AddPeer adds new known addresses to the node. It must update the
	// routing table of the node. Adding ourself should have no effect.
	//
	// - implemented in HW0
	AddPeer(addr ...string)

	// GetRoutingTable returns the node's routing table. It should be a copy.
	//
	// - implemented in HW0
	GetRoutingTable() RoutingTable

	// SetRoutingEntry sets the routing entry. Overwrites it if the entry
	// already exists. If the origin is equal to the relayAddr, then the node
	// has a new neighbor (the notion of neighboors is not needed in HW0). If
	// relayAddr is empty then the record must be deleted (and the peer has
	// potentially lost a neighbor).
	//
	// - implemented in HW0
	SetRoutingEntry(origin, relayAddr string)
}

// RoutingTable defines a simple next-hop routing table. The key is the origin
// and the value the relay address. The routing table must always have an entry
// to itself as follow:
//
//	Table[myAddr] = myAddr.
//
// Table[C] = B means that to reach C, message must be sent to B, the relay.
type RoutingTable map[string]string

func (r RoutingTable) String() string {
	out := new(strings.Builder)

	out.WriteString("Origin\tRelay\n")
	out.WriteString("---\t---\n")

	for origin, relay := range r {
		fmt.Fprintf(out, "%s\t%s\n", origin, relay)
	}

	return out.String()
}

// DisplayGraph displays the routing table as a graphviz graph.
//
//	dot -Tpdf -O *.dot
func (r RoutingTable) DisplayGraph(out io.Writer) {
	fmt.Fprint(out, "digraph routing_table {\n")

	fmt.Fprintf(out, "labelloc=\"t\";")
	fmt.Fprintf(out, "label = <Routing Table <font point-size='10'><br/>"+
		"(generated %s)</font>>;\n\n", time.Now().Format("2 Jan 06 - 15:04:05"))
	fmt.Fprintf(out, "graph [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "graph [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "node [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "edge [fontname = \"helvetica\"];\n\n")

	node := "NODE"

	for origin, relay := range r {
		if origin == relay {
			fmt.Fprintf(out, "\"%s\" -> \"%s\";\n", node, origin)
		} else {
			fmt.Fprintf(out, "\"%s\" -> \"%s\";\n", relay, origin)
		}
	}

	fmt.Fprint(out, "}\n")
}

// SafeRoutingTable define a safe way to acces the RoutingTable.
type SafeRoutingTable struct {
	R  RoutingTable
	mu sync.Mutex
}

func (sr *SafeRoutingTable) String() string { sr.mu.Lock(); defer sr.mu.Unlock(); return sr.R.String() }

func (sr *SafeRoutingTable) DisplayGraph(out io.Writer) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.R.DisplayGraph(out)
}

// SetEntry set a routing entry and override it if the entry already exist
func (sr *SafeRoutingTable) SetEntry(key, str string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.R[key] = str
}

// DeleteEntry delete a routing entry or do nothing if the entry doesn't exist
func (sr *SafeRoutingTable) DeleteEntry(key string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.R, key)
}

// Copy return a deep copy of the RoutingTable
func (sr *SafeRoutingTable) Copy() RoutingTable {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	tableCopy := make(map[string]string)
	for key, value := range sr.R {
		tableCopy[key] = value
	}
	return tableCopy
}

// Get return the value of an entry, bool == false if the entry doesn't exist
func (sr *SafeRoutingTable) Get(key string) (string, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	value, b := sr.R[key]
	return value, b
}
