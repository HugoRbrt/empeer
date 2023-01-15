package types

import (
	"crypto/rsa"
	"go.dedis.ch/cs438/transport"
)

// ChatMessage is a message sent to exchange text messages between nodes.
//
// - implements types.Message
// - implemented in HW0
type ChatMessage struct {
	Message string
}

// RumorsMessage is a type of message that uses gossip mechanisms to ensure
// reliable delivery. It will eventually be distributed over all nodes.
//
// - implements types.Message
// - implemented in HW1
type RumorsMessage struct {
	Rumors []Rumor
}

// Rumor wraps a message to ensure delivery to all peers-
type Rumor struct {
	// Origin is the address of the node that initiated the rumor
	Origin string

	// Sequence is the unique ID of the packet from packet's creator point of
	// view. Each time a sender creates a packet, it must increment its sequence
	// number and include it. Start from 1.
	Sequence uint

	// The message the rumor embeds.
	Msg *transport.Message
}

// AckMessage is an acknowledgement message sent back when a node receives a
// rumor. It servers two purpose: (1) tell that it received the message, and (2)
// share its status.
//
// - implements types.Message
// - implemented in HW1
type AckMessage struct {
	// AckedPacketID is the PacketID this acknowledgment is for
	AckedPacketID string
	Status        StatusMessage
}

// StatusMessage describes a status message. It contains the last known sequence
// for an origin. Status messages are used in Ack and by the anti-entropy.
//
// - implements types.Message
// - implemented in HW1
type StatusMessage map[string]uint

// EmptyMessage describes an empty message. It is used for the heartbeat
// mechanism.
//
// - implements types.Message
// - implemented in HW1
type EmptyMessage struct{}

// PrivateMessage describes a message intended to some specific recipients.
//
// - implements types.Message
// - implemented in HW1
type PrivateMessage struct {
	// Recipients is a bag of recipients
	Recipients map[string]struct{}

	// Msg is the private message to be read by the recipients
	Msg *transport.Message
}

// InstructionMessage gives a list of data for computation on Empeer system
//
// - implements types.Message
// - implemented in PROJECT
type InstructionMessage struct {
	// PacketID to identify each instruction
	PacketID string
	// data to sort
	Data []int
}

// ResultMessage gives a list of sorted data after computation with Empeer system
//
// - implements types.Message
// - implemented in PROJECT
type ResultMessage struct {
	// PacketID is the PacketID this acknowledgment is for
	PacketID string
	// sorted data
	SortData  []int
	Signature []byte
	Hash      []byte
	Pk        *rsa.PublicKey
}

type PublicKeyExchange struct {
	// PublicKey is the public key of the node
	PublicKey *rsa.PublicKey
}

// MRInstructionMessage gives a list of data and a list of reducers for response
//
// - implements types.Message
// - implemented in PROJECT
type MRInstructionMessage struct {
	// RequestID is the ID for the MapReduce request
	RequestID string
	// Reducers is the sorted list of Reducers
	Reducers []string
	// Data is a list of words to count
	Data []string
}

// MRResponseMessage gives a dictionary of processed data
//
// - implements types.Message
// - implemented in PROJECT
type MRResponseMessage struct {
	// requestID is the ID for the MapReduce request
	RequestID string
	// data is a list of words to count
	SortedData map[string]int

	Signature []byte

	Hash []byte
}

// MRInstructionMessage gives a list of data and a list of reducers for response
//
// - implements types.Message
// - implemented in PROJECT
type MRInstructionMessage struct {
	// RequestID is the ID for the MapReduce request
	RequestID string
	// Reducers is the sorted list of Reducers
	Reducers []string
	// Data is a list of words to count
	Data []string
}

// MRResponseMessage gives a dictionary of processed data
//
// - implements types.Message
// - implemented in PROJECT
type MRResponseMessage struct {
	// requestID is the ID for the MapReduce request
	RequestID string
	// data is a list of words to count
	SortedData map[string]int
}
