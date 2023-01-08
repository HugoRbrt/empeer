package types

import (
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------------
// ChatMessage

// NewEmpty implements types.Message.
func (c ChatMessage) NewEmpty() Message {
	return &ChatMessage{}
}

// Name implements types.Message.
func (ChatMessage) Name() string {
	return "chat"
}

// String implements types.Message.
func (c ChatMessage) String() string {
	return fmt.Sprintf("<%s>", c.Message)
}

// HTML implements types.Message.
func (c ChatMessage) HTML() string {
	return c.String()
}

// -----------------------------------------------------------------------------
// RumorsMessage

// NewEmpty implements types.Message.
func (r RumorsMessage) NewEmpty() Message {
	return &RumorsMessage{}
}

// Name implements types.Message.
func (RumorsMessage) Name() string {
	return "rumor"
}

// String implements types.Message.
func (r RumorsMessage) String() string {
	out := new(strings.Builder)
	out.WriteString("rumor{")
	for _, r := range r.Rumors {
		fmt.Fprint(out, r.String())
	}
	out.WriteString("}")
	return out.String()
}

// HTML implements types.Message.
func (r RumorsMessage) HTML() string {
	out := make([]string, len(r.Rumors))
	for i, r := range r.Rumors {
		out[i] = r.String()
	}

	return strings.Join(out, "<br/>")
}

// String implements types.Message.
func (r Rumor) String() string {
	return fmt.Sprintf("{%s-%d-%s}", r.Origin, r.Sequence, r.Msg.Type)
}

// -----------------------------------------------------------------------------
// AckMessage

// NewEmpty implements types.Message.
func (AckMessage) NewEmpty() Message {
	return &AckMessage{}
}

// Name implements types.Message
func (a AckMessage) Name() string {
	return "ack"
}

// String implements types.Message.
func (a AckMessage) String() string {
	return fmt.Sprintf("{ack for packet %s}", a.AckedPacketID)
}

// HTML implements types.Message.
func (a AckMessage) HTML() string {
	return fmt.Sprintf("ack for packet<br/>%s", a.AckedPacketID)
}

// -----------------------------------------------------------------------------
// StatusMessage

// NewEmpty implements types.Message.
func (StatusMessage) NewEmpty() Message {
	return &StatusMessage{}
}

// Name implements types.Message
func (s StatusMessage) Name() string {
	return "status"
}

// String implements types.Message.
func (s StatusMessage) String() string {
	out := new(strings.Builder)

	if len(s) > 5 {
		fmt.Fprintf(out, "{%d elements}", len(s))
	} else {
		for addr, seq := range s {
			fmt.Fprintf(out, "{%s-%d}", addr, seq)
		}
	}

	res := out.String()
	if res == "" {
		res = "{}"
	}

	return res
}

// HTML implements types.Message.
func (s StatusMessage) HTML() string {
	out := new(strings.Builder)

	for addr, seq := range s {
		fmt.Fprintf(out, "{%s-%d}", addr, seq)
	}

	res := out.String()
	if res == "" {
		res = "{}"
	}

	return res
}

// -----------------------------------------------------------------------------
// EmptyMessage

// NewEmpty implements types.Message.
func (EmptyMessage) NewEmpty() Message {
	return &EmptyMessage{}
}

// Name implements types.Message.
func (e EmptyMessage) Name() string {
	return "empty"
}

// String implements types.Message.
func (e EmptyMessage) String() string {
	return "{∅}"
}

// HTML implements types.Message.
func (e EmptyMessage) HTML() string {
	return "{∅}"
}

// -----------------------------------------------------------------------------
// PrivateMessage

// NewEmpty implements types.Message.
func (p PrivateMessage) NewEmpty() Message {
	return &PrivateMessage{}
}

// Name implements types.Message.
func (p PrivateMessage) Name() string {
	return "private"
}

// String implements types.Message.
func (p PrivateMessage) String() string {
	return fmt.Sprintf("private message for %s", p.Recipients)
}

// HTML implements types.Message.
func (p PrivateMessage) HTML() string {
	return fmt.Sprintf("private message for %s", p.Recipients)
}

// -----------------------------------------------------------------------------
// InstructionMessage

// NewEmpty implements types.Message.
func (i InstructionMessage) NewEmpty() Message {
	return &InstructionMessage{}
}

// Name implements types.Message.
func (i InstructionMessage) Name() string {
	return "instructionMessage"
}

// String implements types.Message.
func (i InstructionMessage) String() string {
	return fmt.Sprintf("instruction message number %s", i.PacketID)
}

// HTML implements types.Message.
func (i InstructionMessage) HTML() string {
	return fmt.Sprintf("instruction message number %s", i.PacketID)
}

// -----------------------------------------------------------------------------
// ResultMessage

func (r ResultMessage) NewEmpty() Message {
	return &ResultMessage{}
}

func (r ResultMessage) Name() string {
	return "ResultMessage"
}

func (r ResultMessage) String() string {
	return fmt.Sprintf("instruction message number %s", r.PacketID)
}

func (r ResultMessage) HTML() string {
	return fmt.Sprintf("instruction message number %s", r.PacketID)
}

// -----------------------------------------------------------------------------
// PublicKeyExchange

func (r PublicKeyExchange) NewEmpty() Message {
	return &PublicKeyExchange{}
}

func (r PublicKeyExchange) Name() string {
	return "PublicKeyExchange"
}

func (r PublicKeyExchange) String() string {
	return fmt.Sprintf("instruction message number %s", r.PublicKey)
}

func (r PublicKeyExchange) HTML() string {
	return fmt.Sprintf("instruction message number %s", r.PublicKey)
}

// MRInstructionMessage

func (r MRInstructionMessage) NewEmpty() Message {
	return &MRInstructionMessage{}
}

func (r MRInstructionMessage) Name() string {
	return "MRInstructionMessage"
}

func (r MRInstructionMessage) String() string {
	return fmt.Sprintf("mapReduce instruction message number %s with data %s", r.RequestID, r.Data)
}

func (r MRInstructionMessage) HTML() string {
	return fmt.Sprintf("mapReduce instruction message number %s", r.RequestID)
}

// -----------------------------------------------------------------------------
// MapReduceResponse

func (r MRResponseMessage) NewEmpty() Message {
	return &MRResponseMessage{}
}

func (r MRResponseMessage) Name() string {
	return "MRResponseMessage"
}

func (r MRResponseMessage) String() string {
	return fmt.Sprintf("mapReduce response message number %s", r.requestID)
}

func (r MRResponseMessage) HTML() string {
	return fmt.Sprintf("mapReduce instruction message number %s", r.requestID)
}

// -----------------------------------------------------------------------------
// utility functions

// RumorByOrigin sorts rumor by origin
type RumorByOrigin []Rumor

func (r RumorByOrigin) Len() int {
	return len(r)
}

func (r RumorByOrigin) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RumorByOrigin) Less(i, j int) bool {
	return r[i].Origin < r[j].Origin
}

// ChatByMessage sorts chat message by their message
type ChatByMessage []*ChatMessage

func (c ChatByMessage) Len() int {
	return len(c)
}

func (c ChatByMessage) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ChatByMessage) Less(i, j int) bool {
	return c[i].Message < c[j].Message
}
