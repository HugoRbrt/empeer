package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
	"regexp"
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
			log.Error().Msgf("error: %v", err.Error())
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
	iDontHave := n.CompareView(n.rumors.GetView(), *statusMsg)
	if len(iDontHave) > 0 {
		// has Rumors that the remote peer doesn't have.
		// then send a status message to the remote peer
		err := n.SendView([]string{}, pkt.Header.Source)
		if err != nil {
			return err
		}
	}
	itDoesntHave := n.CompareView(*statusMsg, n.rumors.GetView())
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
	n.waitAck.signalNotif(ackMsg.AckedPacketID, nil)

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
	n.waitAck.signalNotif(dataReplyMsg.RequestID, dataReplyMsg.Value)
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
		_, err = n.shareSearch(budget, *searchRequestMsg, []string{pkt.Header.Source, n.conf.Socket.GetAddress()}, false)
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
	n.fileNotif.sendNotif(searchReplyMsg.RequestID, searchReplyMsg.Responses)
	// update catalog with responses
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

func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	a := n.tlc.a
	a.mu.Lock()
	// cast the message to its actual type. You assume it is the right type.
	paxosPrepareMsg, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	if paxosPrepareMsg.Step != a.step || paxosPrepareMsg.ID <= a.maxID {
		a.mu.Unlock()
		return nil
	}

	// PROMISE response
	a.maxID = paxosPrepareMsg.ID
	promiseMsg := types.PaxosPromiseMessage{
		Step:          paxosPrepareMsg.Step,
		ID:            paxosPrepareMsg.ID,
		AcceptedID:    a.acceptedID,
		AcceptedValue: a.acceptedValue,
	}
	a.mu.Unlock()
	trPromiseMsg, err := a.conf.MessageRegistry.MarshalMessage(&promiseMsg)
	if err != nil {
		return err
	}
	privMsg := types.PrivateMessage{Msg: &trPromiseMsg, Recipients: map[string]struct{}{paxosPrepareMsg.Source: {}}}
	respMsg, err := a.conf.MessageRegistry.MarshalMessage(privMsg)
	if err != nil {
		return err
	}
	go func() {
		err = a.Broadcast(respMsg)
		if err != nil {
			log.Error().Msgf("error to broadcast promise message")
		}
	}()
	return nil
}

func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	p := n.tlc.p
	// cast the message to its actual type. You assume it is the right type.
	paxosPromiseMsg, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if paxosPromiseMsg.Step != p.step || p.phase != 1 {
		return nil
	}
	p.nbResponses++
	if paxosPromiseMsg.AcceptedID > p.maxAcceptedID {
		p.maxAcceptedID = paxosPromiseMsg.AcceptedID
		p.acceptedValue = paxosPromiseMsg.AcceptedValue
	}
	return nil
}

func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	a := n.tlc.a
	// cast the message to its actual type. You assume it is the right type.
	paxosProposeMsg, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	a.mu.Lock()
	if paxosProposeMsg.Step != a.step || paxosProposeMsg.ID != a.maxID {
		a.mu.Unlock()
		return nil
	}
	// ACCEPT response
	a.acceptedID = paxosProposeMsg.ID
	a.acceptedValue = &paxosProposeMsg.Value
	a.mu.Unlock()
	acceptMsg := types.PaxosAcceptMessage{
		Step:  paxosProposeMsg.Step,
		ID:    paxosProposeMsg.ID,
		Value: paxosProposeMsg.Value,
	}
	trAcceptMsg, err := a.conf.MessageRegistry.MarshalMessage(acceptMsg)
	if err != nil {
		return err
	}
	go func() {
		err = a.Broadcast(trAcceptMsg)
		if err != nil {
			log.Error().Msgf("error to broadcast promise message")
		}
	}()
	return nil
}

func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	tlc := n.tlc
	// cast the message to its actual type. You assume it is the right type.
	paxosAcceptMsg, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	tlc.p.mu.Lock()
	if paxosAcceptMsg.Step == tlc.p.step {
		tlc.p.nbResponses++
		tlc.p.acceptedValue = &paxosAcceptMsg.Value
	}
	tlc.mu.Lock()
	if int(tlc.p.nbResponses) >= tlc.p.conf.PaxosThreshold(tlc.p.conf.TotalPeers) && paxosAcceptMsg.Step == tlc.step {
		tlc.mu.Unlock()
		// consensus is reached for the first time!
		v := &paxosAcceptMsg.Value
		tlc.p.mu.Unlock()
		block, err := tlc.NewBlock(v)
		if err != nil {
			return err
		}
		go func() {
			err = tlc.SendNewTLC(block)
			if err != nil {
				log.Error().Msgf(err.Error())
			}
		}()
	} else {
		tlc.mu.Unlock()
		tlc.p.mu.Unlock()
	}
	return nil
}

func (n *node) ExecTLCMessage(msg types.Message, pkt transport.Packet) error {
	tlc := n.tlc
	tlc.muEx.Lock()
	defer tlc.muEx.Unlock()
	// cast the message to its actual type. You assume it is the right type.
	tlcMsg, ok := msg.(*types.TLCMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", tlcMsg)
	}
	tlc.mu.Lock()
	if tlcMsg.Step < tlc.step {
		tlc.mu.Unlock()
		return nil
	}
	// store the message for corresponding step
	v, exist := tlc.Resp[tlcMsg.Step]
	if !exist {
		v.nb = 1
		v.value = tlcMsg.Block
	} else {
		v.nb++
	}
	tlc.Resp[tlcMsg.Step] = v
	// the rest of the work is done on our current step
	v, existCurr := tlc.Resp[tlc.step]
	tlc.mu.Unlock()
	if !existCurr {
		return nil
	}

	if int(v.nb) >= tlc.conf.PaxosThreshold(tlc.conf.TotalPeers) {
		err := n.AtThresholdTLC(false, v.value)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	for {
		tlc.mu.Lock()
		v, exist := tlc.Resp[tlc.step]
		tlc.mu.Unlock()
		if !exist {
			return nil
		}
		if int(v.nb) >= tlc.conf.PaxosThreshold(tlc.conf.TotalPeers) {
			err := n.AtThresholdTLC(true, v.value)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

func (n *node) ExecInstructionMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	instrMsg, ok := msg.(*types.InstructionMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", instrMsg)
	}
	// send public key to the pkt.source
	hdr := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	msgResponse := types.PublicKeyExchange{PublicKey: n.PublicKey}
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(msgResponse)
	if err != nil {
		return err
	}
	pktResponse := transport.Packet{Header: &hdr, Msg: &transMsg}
	err = n.conf.Socket.Send(pkt.Header.Source, pktResponse, time.Millisecond*1000)
	if err != nil {
		return err
	}

	go func() {
		err := n.ComputeEmpeer(*instrMsg, pkt.Header.Source)
		if err != nil {
			log.Error().Msgf(err.Error())
		}
	}()
	return nil
}

func (n *node) ExecResultMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	resMsg, ok := msg.(*types.ResultMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", resMsg)
	}

	// if length of n.ResMap[pkt.Header.PacketID] is less than 3
	// add the data to the map
	n.ResMapMutex.Lock()
	data := NotificationEmpeerData{
		arr:       resMsg.SortData,
		signature: resMsg.Signature,
		ip:        pkt.Header.Source,
		hash:      resMsg.Hash,
		pk:        resMsg.Pk,
	}
	n.ResMap[resMsg.PacketID] = append(n.ResMap[resMsg.PacketID], data)
	// log the data

	if len(n.ResMap[resMsg.PacketID]) == n.MaxNeighboor {
		log.Info().Msgf("Len received for packetId %s %v", pkt.Header.PacketID, len(n.ResMap[resMsg.PacketID]))
		n.waitEmpeer.signalNotif(resMsg.PacketID, n.ResMap[resMsg.PacketID])
	}
	n.ResMapMutex.Unlock()

	return nil
}

func (n *node) ExecPublicKeyExchange(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	resMsg, ok := msg.(*types.PublicKeyExchange)
	if !ok {
		return xerrors.Errorf("wrong type: %T", resMsg)
	}

	n.PublicKeyMapMutex.Lock()
	//if _, ok := n.PublicKeyMap.Get(pkt.Header.Source); !ok { // we set the public key only once
	n.PublicKeyMap.SetEntry(pkt.Header.Source, resMsg.PublicKey)
	//}
	n.PublicKeyMapMutex.Unlock()

	return nil
}

func (n *node) MRInstructionMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	instrMsg, ok := msg.(*types.MRInstructionMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", instrMsg)
	}
	//log.Info().Msgf(instrMsg.String())
	//prepare statement for reducer
	n.empeer.mr.SetParam(instrMsg.RequestID, len(instrMsg.Reducers), pkt.Header.Source)
	// compute and distributes result to reducers
	data := instrMsg.Data
	nbReducers := len(instrMsg.Reducers)
	dictionaries := n.Map(nbReducers, data)
	err := n.DistributeToReducers(dictionaries, instrMsg.Reducers, instrMsg.RequestID)
	if err != nil {
		return err
	}
	return nil
}

func (n *node) MRResponseMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	resMsg, ok := msg.(*types.MRResponseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", resMsg)
	}
	//log.Info().Msgf(n.conf.Socket.GetAddress() + ": " + resMsg.String())
	// store de result
	allDataReceived := n.empeer.mr.DataReceived(resMsg.RequestID, resMsg.SortedData)
	if allDataReceived {
		log.Info().Msgf("all data received")
		//concat all data
		result := n.ConcatResults(resMsg.RequestID)
		initiator := n.empeer.mr.Initiator(resMsg.RequestID)
		if initiator == n.conf.Socket.GetAddress() {
			//if i am the initiator: return the result
			log.Info().Msgf("result: %s", result)
		} else {
			//if i am a reducer: send the result to the initiator
			msg := types.MRResponseMessage{RequestID: resMsg.RequestID, SortedData: result}
			transMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
			if err != nil {
				return err
			}
			header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), initiator, 0)
			pkt := transport.Packet{Header: &header, Msg: &transMsg}
			err = n.conf.Socket.Send(initiator, pkt, time.Millisecond*1000)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
