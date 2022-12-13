package impl

import (
	"errors"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
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

type MergeSort struct {
	alreadytryNeighbor []string
	mu                 sync.Mutex
}

func (ms *MergeSort) Init(n *node) {
	ms.alreadytryNeighbor = append(ms.alreadytryNeighbor, n.conf.Socket.GetAddress())
}

// MASTER VIEW

// MergeSort define the distributed algorithm for a merge sort
func (n *node) MergeSort(data []int) (error, []int) {
	// process computation locally if data is small enough
	if uint(len(data)) <= n.conf.EmpeerThreshold {
		return nil, n.ComputeLocally(data)
	}
	log.Info().Msgf("div computation")
	// divide computation
	middle := len(data) / 2
	data1 := data[:middle]
	data2 := data[middle:]
	errs := make(chan error)
	result1 := make(chan []int)
	result2 := make(chan []int)
	// send instructions
	go func(c chan error) {
		e, r1 := n.SendComputation(data1)
		if e != nil {
			c <- e
		} else {
			result1 <- r1
		}
	}(errs)
	go func(c chan error) {
		e, r2 := n.SendComputation(data2)
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
func (n *node) SendComputation(data []int) (error, []int) {
	var err error
	var result []int
	err = transport.TimeoutError(0)
	for err == transport.TimeoutError(0) {
		// while we don't have any response, we retry with another neighbor
		n.empeer.ms.mu.Lock()
		log.Info().Msgf("already try: %s", n.empeer.ms.alreadytryNeighbor)
		ok, neighbor := n.table.GetRandomNeighbors(n.empeer.ms.alreadytryNeighbor)
		if !ok {
			// if no neighbor was found: do nothing
			n.empeer.ms.mu.Unlock()
			return errors.New("not enough neighbor"), nil
		}
		n.empeer.ms.alreadytryNeighbor = append(n.empeer.ms.alreadytryNeighbor, neighbor)
		n.empeer.ms.mu.Unlock()
		err, result = n.TrySendComputation(data, neighbor)
	}
	log.Info().Msgf("err: %s", err)
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
	log.Info().Msgf("send with packetId: %s", msg.PacketID)
	select {
	case res := <-n.waitEmpeer.waitNotif(msg.PacketID):
		// if result is received: return it
		log.Info().Msgf("result obtain: %s", res)
		return nil, res
	case <-time.After(timeout): // resend the message to another neighbor
		log.Info().Msgf("temps écoulé de %s", timeout)
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
	return time.Second * 2
	//treeHeight := math.Log(float64(length)) / math.Log(2)
	//return n.conf.EmpeerTimeout * (time.Second * time.Duration(int(math.Pow(2, treeHeight))))
}

// SLAVE VIEW

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
	// process computation locally if data is small enough
	if uint(len(instructionMsg.Data)) <= n.conf.EmpeerThreshold {
		result := n.ComputeLocally(instructionMsg.Data)
		return n.SendResponse(result, instructionMsg, master)
	}
	log.Info().Msgf("deep merge sort not already implemented")
	return nil

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
