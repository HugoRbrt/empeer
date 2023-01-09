package impl

import (
	"encoding/hex"
	"errors"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Empeer struct {
	ms MergeSort
	mr MapReduce
}

// Init initialize Empeer
func (e *Empeer) Init(n *node) {
	e.ms.Init(n)
	e.mr.Init(n)
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
	timeout := n.ComputeTimeOut(len(data))
	for err == transport.TimeoutError(0) {
		packetID := xid.New().String()
		for i := 0; i < n.MaxNeighboor; i++ {
			n.empeer.ms.mu.Lock()
			ok, neighbor := n.table.GetRandomNeighbors(n.empeer.ms.alreadytryNeighbor[instructionNb])
			if !ok {
				// if no neighbor was found: do nothing
				n.empeer.ms.mu.Unlock()
				return errors.New("not enough neighbor"), nil
			}
			n.empeer.ms.alreadytryNeighbor[instructionNb] = append(n.empeer.ms.alreadytryNeighbor[instructionNb], neighbor)
			// we have the neighbor public key
			n.empeer.ms.mu.Unlock()

			// send the request using a go function

			err, result = n.TrySendComputation(data, neighbor, packetID)
			if err != nil {
				return err, result
			}
		}

		n.waitEmpeer.requestNotif(packetID)
		select {
		case res := <-n.waitEmpeer.waitNotif(packetID):
			var hashArray []string
			for i := range res { // get all the hash of the response
				h := n.ComputeHashKeyForList(res[i].arr)
				hashArray = append(hashArray, hex.EncodeToString(h))
			}

			// check if at least two hashes are the same, if only one hash return true
			ok, trustedH := atLeastTwoItemsAreSame(hashArray)
			if !ok {
				return errors.New("the 3 hashes are differents"), nil
			}

			if !allItemsAreTheSame(hashArray) {
				log.Info().Msgf("The three hashes are not the same but at least two are identical -> accepted")
			}

			trustedHByte, err := hex.DecodeString(trustedH)
			if err != nil {
				return err, nil
			}

			var re NotificationEmpeerData
			var errorFound bool
			errorFound = true
			for i := range res {
				if compareTwoArray(res[i].hash, trustedHByte) {
					re = res[i]
					errorFound = false
				}
			}

			if errorFound {
				return errors.New("invalid hash"), nil
			}

			pubKey, ok := n.PublicKeyMap.Get(re.ip)
			if !ok {
				return errors.New("public key not found"), nil
			}

			//log.Log().Msgf("%s received %v from %s", n.conf.Socket.GetAddress(), re.arr, re.ip)
			if !n.VerifySignature(re.signature, re.hash, pubKey) {
				return errors.New("signature not valid"), nil
			}

			return nil, re.arr

		case <-time.After(timeout): // resend the message to another neighbor
			n.waitEmpeer.deleteNotif(packetID)
			err = transport.TimeoutError(0)
			log.Info().Msgf("timeout")
			break
		}
	}
	return err, result
}

// TrySendComputation send an instruction message to the neighbour en wait until  timeout is finished
func (n *node) TrySendComputation(data []int, neighbor, packetID string) (error, []int) {

	// create the message
	msg := types.InstructionMessage{
		PacketID: packetID,
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
	return nil, nil
}
func compareTwoArray(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func atLeastTwoItemsAreSame(arr []string) (bool, string) {
	// Create a map to count the occurrences of each element in the array
	if len(arr) == 1 {
		return true, arr[0]
	}
	occurrences := make(map[string]int)
	for _, v := range arr {
		occurrences[v]++
	}

	// Check if at least two elements are the same
	atLeastTwoSame := false
	var mostFrequent string
	for k, v := range occurrences {
		if v >= 2 {
			atLeastTwoSame = true
			mostFrequent = k
			break
		}
	}

	// Return the result and the most frequent element
	return atLeastTwoSame, mostFrequent
}

// create a boolean function to check if three items in a string array are the same
func allItemsAreTheSame(arr []string) bool {
	// Create a map to count the occurrences of each element in the array
	occurrences := make(map[string]int)
	for _, v := range arr {
		occurrences[v]++
	}

	return len(occurrences) == 1
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
	return n.conf.EmpeerTimeout * time.Duration(math.RoundToEven(float64(length)*math.Log(float64(length))/float64(n.conf.EmpeerThreshold))) * 5000
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
	// log data variable
	//log.Error().Msgf("data value %v", data)
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
	if n.conf.MaliciousNode {
		// shuffle sortedData
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(sortedData), func(i, j int) { sortedData[i], sortedData[j] = sortedData[j], sortedData[i] })
	}
	//
	// compute hash of list of integers
	hash := n.ComputeHashKeyForList(sortedData)
	// sign the hash
	signature := n.SignHash(hash, n.PrivateKey)

	// create the message
	response := types.ResultMessage{
		PacketID:  instructionMsg.PacketID,
		SortData:  sortedData,
		Signature: signature,
		Hash:      hash,
		Pk:        n.PublicKey,
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

type MapReduceParameter struct {
	nbReducer int
	initiator string
}

type MapReduce struct {
	// params map a RequestID to corresponding parameters (number of reducer for the request and if I am the initiator)
	params map[string]MapReduceParameter
	// receivedData store a list of data (every data is a map[string]int) according to its corresponding requestID (string)
	receivedData map[string][]map[string]int
	mu           sync.Mutex
	// isRunning avoids the case where a node wants to saturate the network by sending several mapreduce requests
	isRunning sync.Mutex
	// result is the result of mapreduce request
	result chan map[string]int
}

func (mr *MapReduce) Init(n *node) {
	mr.params = make(map[string]MapReduceParameter)
	mr.receivedData = make(map[string][]map[string]int)
	mr.result = make(chan map[string]int)
}

func (mr *MapReduce) SetParam(requestID string, nbReducer int, initiator string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	var param MapReduceParameter
	param.nbReducer = nbReducer
	param.initiator = initiator
	mr.params[requestID] = param
}

// DataReceived keep received data in memory and return if all the data for this request was received
func (mr *MapReduce) DataReceived(requestID string, data map[string]int) bool {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.receivedData[requestID] = append(mr.receivedData[requestID], data)
	return len(mr.receivedData[requestID]) == mr.params[requestID].nbReducer
}

func (mr *MapReduce) Initiator(requestID string) string {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return mr.params[requestID].initiator
}

// MapReduce begin the mapreduce algorithm with nbMapper mappers on data
func (n *node) MapReduce(nbMapper int, data []string) (error, map[string]int) {
	// signal that a mapreduce begin
	n.empeer.mr.isRunning.Lock()
	defer n.empeer.mr.isRunning.Unlock()
	err := n.SplitSendData(nbMapper, data)
	if err != nil {
		return err, nil
	}
	// catch the result
	select {
	case r := <-n.empeer.mr.result:
		return nil, r
	case <-n.ctx.Done():
		return nil, nil
	}
}

// SplitSendData split data and send to nbMapper =/= neighbors randomly
func (n *node) SplitSendData(nbMapper int, data []string) error {
	// get nbMapper =/= neighbors
	var mappers []string
	for k := 0; k < nbMapper; k++ {
		ok, neighbor := n.table.GetRandomNeighbors(append(mappers, n.conf.Socket.GetAddress()))
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
		//choose which data to send
		log.Info().Msgf("%s", numMapper)
		data := listData[numMapper]
		//send data to mapper
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
	//prepare statement for reducer
	n.empeer.mr.SetParam(requestID, nbMapper, n.conf.Socket.GetAddress())
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
		//send data to the corresponding reducer
		msg := types.MRResponseMessage{RequestID: requestID, SortedData: dict}
		// TODO: Sign msg before sending
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

// ConcatResults concatenate all received dictionaries for the corresponding requestID
func (n *node) ConcatResults(requestID string) map[string]int {
	n.empeer.mr.mu.Lock()
	defer n.empeer.mr.mu.Unlock()
	dictionaries := n.empeer.mr.receivedData[requestID]
	result := make(map[string]int)
	for _, dict := range dictionaries {
		for key, value := range dict {
			result[key] += value
		}
	}
	return result
}
