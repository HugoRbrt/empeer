package impl

import (
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

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
		n.waitAck.requestNotif(pktRelay.Header.PacketID)
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
	n.waitAck.signalNotif(ackMsg.AckedPacketID)

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

func (n *node) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	dataReplyMsg, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// give a response:
	hdr := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	nextHop, exist := n.table.Get(pkt.Header.RelayedBy)
	if !exist {
		return xerrors.Errorf("unknown destination address for response")
	}
	msgResponse := types.DataReplyMessage{
		RequestID: dataReplyMsg.RequestID,
		Key:       dataReplyMsg.Key,
		Value:     n.conf.Storage.GetDataBlobStore().Get(dataReplyMsg.Key),
	}
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(msgResponse)
	if err != nil {
		return err
	}
	pktResponse := transport.Packet{Header: &hdr, Msg: &transMsg}

	// send msg
	err = n.conf.Socket.Send(nextHop, pktResponse, time.Millisecond*1000)
	return err
}

func (n *node) ExecDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	dataReplyMsg, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	_ = pkt
	// stops the timer and send obtained value
	n.waitAck.sendNotif(dataReplyMsg.RequestID, dataReplyMsg.Value)
	n.waitAck.signalNotif(dataReplyMsg.RequestID)
	return nil
}

func (n *node) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	searchRequestMsg, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	budget := searchRequestMsg.Budget - 1
	reg, err := regexp.Compile(searchRequestMsg.Pattern)
	if err != nil {
		return err
	}
	// forward search
	if budget > 0 {
		err = n.PropagateSearchAll(*reg, budget, *searchRequestMsg, []string{pkt.Header.Source, n.conf.Socket.GetAddress()})
		if err != nil {
			return err
		}
	}
	// construct fileInfo
	files := n.searchLocally(*reg, false)
	// reply to the source
	hdr := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), searchRequestMsg.Origin, 0)
	msgResponse := types.SearchReplyMessage{RequestID: searchRequestMsg.RequestID, Responses: files}
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(msgResponse)
	if err != nil {
		return err
	}
	pktResponse := transport.Packet{Header: &hdr, Msg: &transMsg}
	err = n.conf.Socket.Send(pkt.Header.Source, pktResponse, time.Millisecond*1000)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	searchReplyMsg, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// stops the timer and send obtained value
	newId := xid.New().String()
	n.waitAck.requestNotif(newId)
	var listNames []string
	for _, f := range searchReplyMsg.Responses {
		listNames = append(listNames, f.Name)
	}
	names := strings.Join(listNames, peer.MetafileSep)
	response := []byte(names + peer.MetafileSep + newId)
	n.waitAck.sendNotif(searchReplyMsg.RequestID, response)

	// TODO: remove this waitNotif
	/*
		select {
		case <-n.ctx.Done():
			return nil
		case <-n.waitAck.waitNotif(newId):
			for _, f := range searchReplyMsg.Responses {
				// update NamingStore
				err := n.Tag(f.Name, f.Metahash)
				if err != nil {
					return err
				}
				n.UpdateCatalog(f.Metahash, pkt.Header.Source)
				//update catalog
				for _, chunk := range f.Chunks {
					if chunk != nil {
						n.UpdateCatalog(string(chunk), pkt.Header.Source)
					}
				}
			}

		}*/

	for _, f := range searchReplyMsg.Responses {
		// update NamingStore
		err := n.Tag(f.Name, f.Metahash)
		if err != nil {
			return err
		}
		n.UpdateCatalog(f.Metahash, pkt.Header.Source)
		//update catalog
		for _, chunk := range f.Chunks {
			if chunk != nil {
				n.UpdateCatalog(string(chunk), pkt.Header.Source)
			}
		}
	}
	return nil
}
