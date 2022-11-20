package impl

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"strconv"
	"sync"
	"time"
)

// Acceptor

// Acceptor define a node with the acceptor role in Paxos
type Acceptor struct {
	*node

	maxID         uint
	step          uint
	acceptedID    uint
	acceptedValue *types.PaxosValue
	mu            sync.Mutex
}

// NewAcceptor permit to create an acceptor role for the node at step s
func (n *node) NewAcceptor(s uint) (a *Acceptor) {
	return &Acceptor{
		node:          n,
		maxID:         0,
		step:          s,
		acceptedValue: nil,
		acceptedID:    0,
	}
}

// NextStep update the acceptor's step
func (a *Acceptor) NextStep() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.step++
	a.acceptedValue = nil
}

// Proposer

// Proposer define a node with the proposer role in Paxos
type Proposer struct {
	*node

	step          uint
	nbResponses   uint
	id            uint
	maxAcceptedID uint
	acceptedValue *types.PaxosValue
	proposedValue *types.PaxosValue
	phase         uint
	mu            sync.Mutex
}

// NewProposer permit to create an acceptor role for the node at step s
func (n *node) NewProposer(s uint) *Proposer {
	return &Proposer{
		node:          n,
		step:          s,
		nbResponses:   0,
		maxAcceptedID: 0,
		acceptedValue: nil,
		proposedValue: nil,
		phase:         1,
		id:            n.conf.PaxosID,
	}
}

// NextStep update the proposer's step
func (p *Proposer) NextStep() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.step++
	p.acceptedValue = nil
}

// SendPrepare send Paxos Prepare message, and return if Proposer received enough Promises
func (p *Proposer) SendPrepare() error {
	p.phase = 1
	// Send prepare message while we don't have a majority of promises
	for {
		// create Prepare Msg
		prepareMsg := types.PaxosPrepareMessage{
			Step:   p.step,
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
		channel := time.After(p.conf.PaxosProposerRetry)
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
			case <-channel:
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
		Step:  p.step,
		ID:    p.id,
		Value: *value,
	}
	p.mu.Unlock()
	// Send propose message while we don't have a majority of accept
	msg, err := p.conf.MessageRegistry.MarshalMessage(proposeMsg)
	if err != nil {
		return false, err
	}
	err = p.Broadcast(msg)
	if err != nil {
		log.Error().Msgf("error to broadcast tlc message")
	}
	//observe if accept responses represent a majority
	for {
		p.mu.Lock()
		if int(p.nbResponses) >= p.conf.PaxosThreshold(p.conf.TotalPeers) {
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

// ProposeConsensus initiate a consensus with the proposer role
func (p *Proposer) ProposeConsensus(proposedValue types.PaxosValue) error {
	p.mu.Lock()
	p.proposedValue = &proposedValue
	p.mu.Unlock()
	for {
		// begin phase 1
		err := p.SendPrepare()
		if err != nil {
			return err
		}
		select {
		case <-p.ctx.Done():
			return nil
		default:
		}
		//begin Phase 2
		v := &proposedValue
		p.mu.Lock()
		if p.acceptedValue != nil {
			v = p.acceptedValue
		}
		p.mu.Unlock()
		result, err := p.SendPropose(v)
		if err != nil {
			return err
		}
		if result {
			// consensus is reached!
			return nil
		}
		p.phase = 1
		// prepare message to retry sending Prepare message
		p.mu.Lock()
		p.id += p.conf.TotalPeers
		p.mu.Unlock()
	}
}

// TLC

// TLC define a node with the tlc role
type TLC struct {
	*node
	p           *Proposer
	a           *Acceptor
	step        uint
	broadcasted bool
	mu          sync.Mutex
	muEx        sync.Mutex
	Resp        map[uint]struct {
		nb    uint
		value types.BlockchainBlock
	}
}

// NewTLC create a tlc role for the node
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

// NextStep update the tlc's step
func (tlc *TLC) NextStep() {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	tlc.step++
	tlc.broadcasted = false
	tlc.a.NextStep()
	tlc.p.NextStep()
}

// NewBlock create a new BlockchainBlock
func (tlc *TLC) NewBlock(value *types.PaxosValue) (types.BlockchainBlock, error) {
	tlc.mu.Lock()
	newBlock := types.BlockchainBlock{
		Index:    tlc.step,
		Hash:     nil,
		Value:    *value,
		PrevHash: tlc.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey),
	}
	tlc.mu.Unlock()
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

// AddBlock add the block to its own blockchain and store fileName/MetaHash association
func (n *node) AddBlock(block types.BlockchainBlock) error {
	buf, err := block.Marshal()
	if err != nil {
		return err
	}
	n.conf.Storage.GetBlockchainStore().Set(hex.EncodeToString(block.Hash), buf)
	n.conf.Storage.GetBlockchainStore().Set(storage.LastBlockKey, block.Hash)
	name := block.Value.Filename
	b := []byte(block.Value.Metahash)
	n.conf.Storage.GetNamingStore().Set(name, b)
	return nil
}

// SendNewTLC send a tlc message
func (tlc *TLC) SendNewTLC(block types.BlockchainBlock) error {
	tlc.mu.Lock()
	// Broadcast TLC msg
	msg := types.TLCMessage{
		Step:  block.Index,
		Block: block,
	}
	transpMsg, err := tlc.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}
	tlc.broadcasted = true
	tlc.mu.Unlock()
	go func() {
		err = tlc.Broadcast(transpMsg)
		if err != nil {
			log.Error().Msgf("error to broadcast tlc message")
		}
	}()
	return nil
}

// LaunchConsensus initiate a consensus, terminates when the filename is the one proposed
func (tlc *TLC) LaunchConsensus(value types.PaxosValue) error {
	oursName := value.Filename
	NamingStore := tlc.conf.Storage.GetNamingStore()
	for {
		if NamingStore.Get(oursName) != nil {
			return xerrors.Errorf("name already exists: %s", NamingStore.Get(oursName))
		}
		err := tlc.p.ProposeConsensus(value)
		if err != nil {
			return err
		}
		if value.Filename == oursName {
			return nil
		}
	}
}

func (n *node) AtThresholdTLC(catchup bool, block types.BlockchainBlock) error {
	// step 1&2
	err := n.AddBlock(block)
	if err != nil {
		return err
	}
	if !catchup {
		// step 3
		n.tlc.mu.Lock()
		if !n.tlc.broadcasted {
			n.tlc.mu.Unlock()
			err = n.tlc.SendNewTLC(block)
			if err != nil {
				return err
			}
		} else {
			n.tlc.mu.Unlock()
		}
	}
	// step 4
	n.tlc.NextStep()
	return nil
}
