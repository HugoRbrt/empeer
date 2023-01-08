package impl

import (
	"errors"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"math"
	"sync"
	"time"
)

type Empeer struct {
	ms MergeSort
}

// Init initialize Empeer
func (e *Empeer) Init(n *node) {
	e.ms.Init(n)
}

// MERGE SORT

type MergeSort struct {
	alreadytryNeighbor map[string][]string
	mu                 sync.Mutex
}

func (ms *MergeSort) Init(n *node) {
	ms.alreadytryNeighbor = make(map[string][]string)
}

// Master view

// MergeSort define the distributed algorithm for a merge sort
func (n *node) MergeSort(data []int) (error, []int) {
	// process computation locally if data is small enough
	if uint(len(data)) <= n.conf.EmpeerThreshold {
		return nil, n.ComputeLocally(data)
	}
	// divide computation
	middle := len(data) / 2
	data1 := data[:middle]
	data2 := data[middle:]
	errs := make(chan error)
	result1 := make(chan []int)
	result2 := make(chan []int)
	// send instructions
	instrNb := xid.New().String()
	n.empeer.ms.alreadytryNeighbor[instrNb] = []string{n.conf.Socket.GetAddress()}
	go func(c chan error) {
		e, r1 := n.SendComputation(data1, instrNb)
		if e != nil {
			c <- e
		} else {
			result1 <- r1
		}
	}(errs)
	go func(c chan error) {
		e, r2 := n.SendComputation(data2, instrNb)
		if e != nil {
			c <- e
		} else {
			result2 <- r2
		}
	}(errs)
	// handle errors and get the result
	nbRes := 0
	var r1 []int
	var r2 []int
	for nbRes < 2 {
		select {
		case r1 = <-result1:
			nbRes++
		case r2 = <-result2:
			nbRes++
		case e := <-errs:
			log.Error().Msgf(e.Error())
			return e, nil
		default:
		}
	}
	return nil, n.MergeData(r1, r2)
}

// SendComputation send a request to neighbors to sort the data, return sorted result
func (n *node) SendComputation(data []int, instructionNb string) (error, []int) {
	var err error
	var result []int
	err = transport.TimeoutError(0)
	for err == transport.TimeoutError(0) {
		// while we don't have any response, we retry with another neighbor
		n.empeer.ms.mu.Lock()
		ok, neighbor := n.table.GetRandomNeighbors(n.empeer.ms.alreadytryNeighbor[instructionNb])
		if !ok {
			// if no neighbor was found: do nothing
			n.empeer.ms.mu.Unlock()
			return errors.New("not enough neighbor"), nil
		}
		n.empeer.ms.alreadytryNeighbor[instructionNb] = append(n.empeer.ms.alreadytryNeighbor[instructionNb], neighbor)
		n.empeer.ms.mu.Unlock()
		err, result = n.TrySendComputation(data, neighbor)
	}
	return err, result
}

// TrySendComputation send an instruction message to the neighbour en wait until  timeout is finished
func (n *node) TrySendComputation(data []int, neighbor string) (error, []int) {
	timeout := n.ComputeTimeOut(len(data))
	// create the message
	msg := types.InstructionMessage{
		PacketID: xid.New().String(),
		Data:     data,
	}
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err, nil
	}
	privMsg := types.PrivateMessage{
		Msg:        &transMsg,
		Recipients: map[string]struct{}{neighbor: {}},
	}
	transPrivMsg, err := n.conf.MessageRegistry.MarshalMessage(privMsg)
	if err != nil {
		return err, nil
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
	pkt := transport.Packet{Header: &header, Msg: &transPrivMsg}
	// send the message
	err = n.conf.Socket.Send(neighbor, pkt, time.Millisecond*1000)
	if err != nil {
		return err, nil
	}
	//wait the response
	n.waitEmpeer.requestNotif(msg.PacketID)
	select {
	case res := <-n.waitEmpeer.waitNotif(msg.PacketID):
		// if result is received: return it
		return nil, res
	case <-time.After(timeout): // resend the message to another neighbor
		return transport.TimeoutError(0), nil
	}
}

// MergeData merge two sorted data into one
func (n *node) MergeData(left []int, right []int) (result []int) {
	result = make([]int, len(left)+len(right))

	i := 0
	for len(left) > 0 && len(right) > 0 {
		if left[0] < right[0] {
			result[i] = left[0]
			left = left[1:]
		} else {
			result[i] = right[0]
			right = right[1:]
		}
		i++
	}

	for j := 0; j < len(left); j++ {
		result[i] = left[j]
		i++
	}
	for j := 0; j < len(right); j++ {
		result[i] = right[j]
		i++
	}

	return
}

func (n *node) ComputeTimeOut(length int) time.Duration {
	return n.conf.EmpeerTimeout * time.Duration(math.RoundToEven(float64(length)*math.Log(float64(length))/float64(n.conf.EmpeerThreshold)))
}

// Slave view

func (n *node) ComputeLocally(data []int) []int {
	var num = len(data)

	if num == 1 {
		return data
	}

	middle := num / 2
	var (
		left  = make([]int, middle)
		right = make([]int, num-middle)
	)
	for i := 0; i < num; i++ {
		if i < middle {
			left[i] = data[i]
		} else {
			right[i-middle] = data[i]
		}
	}

	return n.MergeData(n.ComputeLocally(left), n.ComputeLocally(right))
}

func (n *node) ComputeEmpeer(instructionMsg types.InstructionMessage, master string) error {
	data := instructionMsg.Data
	// process computation locally if data is small enough
	if uint(len(instructionMsg.Data)) <= n.conf.EmpeerThreshold {
		result := n.ComputeLocally(instructionMsg.Data)
		return n.SendResponse(result, instructionMsg, master)
	}
	// divide computation
	middle := len(data) / 2
	data1 := data[:middle]
	data2 := data[middle:]
	errs := make(chan error)
	result1 := make(chan []int)
	result2 := make(chan []int)
	// send instructions
	n.empeer.ms.alreadytryNeighbor[instructionMsg.PacketID] = []string{n.conf.Socket.GetAddress()}
	go func(c chan error) {
		e, r1 := n.SendComputation(data1, instructionMsg.PacketID)
		if e != nil {
			c <- e
		} else {
			result1 <- r1
		}
	}(errs)
	go func(c chan error) {
		e, r2 := n.SendComputation(data2, instructionMsg.PacketID)
		if e != nil {
			c <- e
		} else {
			result2 <- r2
		}
	}(errs)
	// handle errors and get the result
	nbRes := 0
	var r1 []int
	var r2 []int
	for nbRes < 2 {
		select {
		case r1 = <-result1:
			nbRes++
		case r2 = <-result2:
			nbRes++
		case e := <-errs:
			log.Error().Msgf(e.Error())
			return e
		default:
		}
	}
	result := n.MergeData(r1, r2)
	return n.SendResponse(result, instructionMsg, master)
}

func (n *node) SendResponse(sortedData []int, instructionMsg types.InstructionMessage, origin string) error {
	// create the message
	response := types.ResultMessage{
		PacketID: instructionMsg.PacketID,
		SortData: sortedData,
	}
	transMsg, err := n.conf.MessageRegistry.MarshalMessage(response)
	if err != nil {
		return err
	}
	privMsg := types.PrivateMessage{
		Msg:        &transMsg,
		Recipients: map[string]struct{}{origin: {}},
	}
	transPrivMsg, err := n.conf.MessageRegistry.MarshalMessage(privMsg)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), origin, 0)
	pkt := transport.Packet{Header: &header, Msg: &transPrivMsg}
	// send the message
	return n.conf.Socket.Send(origin, pkt, time.Millisecond*1000)
}

// MAP REDUCE

// MapReduce begin the mapreduce algorithm with nbMapper mappers on data
func (n *node) MapReduce(nbMapper int, data []string) error {
	return n.SplitSendData(nbMapper, data)
}

// SplitSendData split data and send to nbMapper =/= neighbors randomly
func (n *node) SplitSendData(nbMapper int, data []string) error {
	// get nbMapper =/= neighbors
	var mappers []string
	for k := 0; k < nbMapper; k++ {
		ok, neighbor := n.table.GetRandomNeighbors(mappers)
		if !ok {
			return errors.New("not enough neighbors")
		}
		mappers = append(mappers, neighbor)
	}
	// split data list into nbMapper lists
	// TODO: test function below
	listData := n.empeer.splitList(data, nbMapper)
	requestID := xid.New().String()
	for numMapper, mapper := range mappers {
		// TODO: choose which data to send
		data := listData[numMapper]
		//TODO: send data to mapper
		msg := types.MRInstructionMessage{RequestID: requestID, Reducers: mappers, Data: data}
		transMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
		if err != nil {
			return err
		}
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), mapper, 0)
		pkt := transport.Packet{Header: &header, Msg: &transMsg}
		err = n.conf.Socket.Send(mapper, pkt, time.Millisecond*1000)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *node) Map(nbReducers int, data []string) []map[string]int {
	listMaps := make([]map[string]int, nbReducers)
	var keys []string
	// 97 is the value for 'a', 26 is the number of letters in the alphabet
	widthReducer := 27 / nbReducers
	if 27%nbReducers != 0 {
		widthReducer += 1
	}
	for _, d := range data {
		index := (int(d[0]) - 97) / widthReducer
		if _, ok := listMaps[index][d]; ok {
			listMaps[index][d]++
		} else {
			correspondingMap := listMaps[index]
			if correspondingMap == nil {
				listMaps[index] = make(map[string]int)
			}
			listMaps[index][d] = 1
			keys = append(keys, d)
		}
	}
	return listMaps
}

func (n *node) DistributeToReducers(dicts []map[string]int, reducers []string, requestID string) error {
	for num, dict := range dicts {
		reducer := reducers[num]
		//TODO: send data to the corresponding reducer
		msg := types.MRResponseMessage{RequestID: requestID, SortedData: dict}
		transMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
		if err != nil {
			return err
		}
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), reducer, 0)
		pkt := transport.Packet{Header: &header, Msg: &transMsg}
		err = n.conf.Socket.Send(reducer, pkt, time.Millisecond*1000)
		if err != nil {
			return err
		}
	}
	return nil
}
