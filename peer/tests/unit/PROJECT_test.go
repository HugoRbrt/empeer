package unit

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/peer/impl"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/channel"
)

// P-1
//
// node launch a local mergesort with small enough data
func Test_PROJECT_local_computation(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	err, result := node1.MergeSort([]int{3, 2, 2, 5})
	require.Equal(t, err, nil)
	require.Equal(t, result, []int{2, 2, 3, 5})
}

// P-2
//
// node launch a shallow mergesort (slave don't became a master)
// ┌───► B
// A───► C
func Test_PROJECT_shallow_computation(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Millisecond*500))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Millisecond*500))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Millisecond*500))
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())

	time.Sleep(time.Millisecond * 1000)

	err, result := node1.MergeSort([]int{3, 2, 2, 5, 1, 3, 4})

	require.Equal(t, err, nil)
	require.Equal(t, result, []int{1, 2, 2, 3, 3, 4, 5})
}

// P-3
//
// node launch a semi-deep mergesort (slave became a master only on one layer)
// ┌───► B
// A───► C
func Test_PROJECT_semi_deep_computation(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node1.GetAddr())
	node3.AddPeer(node2.GetAddr())

	err, result := node1.MergeSort([]int{5, 1, 2, 4, 3, 4, 2, 5, 3, 1, 9, 0})
	require.Equal(t, nil, err)
	require.Equal(t, result, []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 9})
}

// P-4
//
// node launch a deep mergesort (slave became a master on several layers)
// ┌───► B
// A───► C
func Test_PROJECT_deep_computation(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node1.GetAddr())
	node3.AddPeer(node2.GetAddr())

	err, result := node1.MergeSort([]int{5, 1, 2, 4, 3, 4, 2, 5, 3, 1, 9, 0, 2, 7, 4, 9, 6, 10, 0, -1, 6, 4, 8})
	require.Equal(t, nil, err)
	require.Equal(t, result, []int{-1, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 4, 5, 5, 6, 6, 7, 8, 9, 9, 10})
}

// P-5
//
// test the ComputeHashKeyForList function
func Test_PROJECT_local_sign(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	// create Hash of the message
	list := []int{3, 2, 2, 5}
	hList := node1.ComputeHashKeyForList(list)
	hlist_ := node1.ComputeHashKeyForList(list)

	require.Equal(t, hList, hlist_)
	list2 := []int{3, 2, 2, 5, 8}
	hList2 := node1.ComputeHashKeyForList(list2)

	// sign the hash for hList
	sign := node1.SignHash(hList, node1.GetPrivateKey())

	// verify the signature for hList should return true
	require.True(t, node1.VerifySignature(sign, hList, node1.GetPublicKey()))

	// verify the signature for hList2 should return false
	require.False(t, node1.VerifySignature(sign, hList2, node1.GetPublicKey()))
}

// P-6
//
// test the ComputeHashKeyForMap function
func Test_PROJECT_local_sign_map(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	// create Hash of the message
	list := map[string]int{"a": 3, "b": 2, "c": 2, "d": 5}
	hList := node1.ComputeHashKeyForMap(list)
	hList_ := node1.ComputeHashKeyForMap(list)

	require.Equal(t, hList, hList_)

	list2 := map[string]int{"a": 3, "b": 2, "c": 2, "d": 5, "e": 8}
	hList2 := node1.ComputeHashKeyForMap(list2)

	// sign the hash for hList
	sign := node1.SignHash(hList, node1.GetPrivateKey())

	// verify the signature for hList should return true
	require.True(t, node1.VerifySignature(sign, hList, node1.GetPublicKey()))

	// verify the signature for hList2 should return false
	require.False(t, node1.VerifySignature(sign, hList2, node1.GetPublicKey()))
}

// P-7
//
// node launch a deep mergesort (slave became a master on several layers) with consensus
func Test_PROJECT_deep_computation_with_consensus(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node4.Stop()

	node5 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node5.Stop()

	node6 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node6.Stop()

	node7 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node7.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node1.AddPeer(node4.GetAddr())
	node1.AddPeer(node5.GetAddr())
	node1.AddPeer(node6.GetAddr())
	node1.AddPeer(node7.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node2.AddPeer(node5.GetAddr())
	node2.AddPeer(node6.GetAddr())
	node2.AddPeer(node7.GetAddr())
	node3.AddPeer(node1.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node3.AddPeer(node4.GetAddr())
	node3.AddPeer(node5.GetAddr())
	node3.AddPeer(node6.GetAddr())
	node3.AddPeer(node7.GetAddr())
	node4.AddPeer(node1.GetAddr())
	node4.AddPeer(node2.GetAddr())
	node4.AddPeer(node3.GetAddr())
	node4.AddPeer(node5.GetAddr())
	node4.AddPeer(node6.GetAddr())
	node4.AddPeer(node7.GetAddr())
	node5.AddPeer(node1.GetAddr())
	node5.AddPeer(node2.GetAddr())
	node5.AddPeer(node3.GetAddr())
	node5.AddPeer(node4.GetAddr())
	node5.AddPeer(node6.GetAddr())
	node5.AddPeer(node7.GetAddr())
	node6.AddPeer(node1.GetAddr())
	node6.AddPeer(node2.GetAddr())
	node6.AddPeer(node3.GetAddr())
	node6.AddPeer(node4.GetAddr())
	node6.AddPeer(node5.GetAddr())
	node6.AddPeer(node7.GetAddr())
	node7.AddPeer(node1.GetAddr())
	node7.AddPeer(node2.GetAddr())
	node7.AddPeer(node3.GetAddr())
	node7.AddPeer(node4.GetAddr())
	node7.AddPeer(node5.GetAddr())
	node7.AddPeer(node6.GetAddr())

	err, result := node1.MergeSort([]int{5, 1, 2, 4, 3, 4, 2, 5, 3, 1, 9, 0, 2, 7, 4, 9, 6, 10, 0, -1, 6, 4, 8})
	require.Equal(t, nil, err)
	require.Equal(t, result, []int{-1, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 4, 5, 5, 6, 6, 7, 8, 9, 9, 10})
}

// P-8
//
// node launch a deep mergesort (slave became a master on several layers) with consensus
// one node is a malicious, the merge sort still need to perform
func Test_PROJECT_deep_computation_with_consensus_one_malicious(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node4.Stop()

	node5 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node5.Stop()

	node6 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node6.Stop()

	node7 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true), z.WithMaliciousNode(true))
	defer node7.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node1.AddPeer(node4.GetAddr())
	node1.AddPeer(node5.GetAddr())
	node1.AddPeer(node6.GetAddr())
	node1.AddPeer(node7.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node2.AddPeer(node5.GetAddr())
	node2.AddPeer(node6.GetAddr())
	node2.AddPeer(node7.GetAddr())
	node3.AddPeer(node1.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node3.AddPeer(node4.GetAddr())
	node3.AddPeer(node5.GetAddr())
	node3.AddPeer(node6.GetAddr())
	node3.AddPeer(node7.GetAddr())
	node4.AddPeer(node1.GetAddr())
	node4.AddPeer(node2.GetAddr())
	node4.AddPeer(node3.GetAddr())
	node4.AddPeer(node5.GetAddr())
	node4.AddPeer(node6.GetAddr())
	node4.AddPeer(node7.GetAddr())
	node5.AddPeer(node1.GetAddr())
	node5.AddPeer(node2.GetAddr())
	node5.AddPeer(node3.GetAddr())
	node5.AddPeer(node4.GetAddr())
	node5.AddPeer(node6.GetAddr())
	node5.AddPeer(node7.GetAddr())
	node6.AddPeer(node1.GetAddr())
	node6.AddPeer(node2.GetAddr())
	node6.AddPeer(node3.GetAddr())
	node6.AddPeer(node4.GetAddr())
	node6.AddPeer(node5.GetAddr())
	node6.AddPeer(node7.GetAddr())
	node7.AddPeer(node1.GetAddr())
	node7.AddPeer(node2.GetAddr())
	node7.AddPeer(node3.GetAddr())
	node7.AddPeer(node4.GetAddr())
	node7.AddPeer(node5.GetAddr())
	node7.AddPeer(node6.GetAddr())

	err, result := node1.MergeSort([]int{5, 1, 2, 4, 3, 4, 2, 5, 3, 1, 9, 0, 2, 7, 4, 9, 6, 10, 0, -1, 6, 4, 8})
	require.Equal(t, nil, err)
	require.Equal(t, []int{-1, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 4, 5, 5, 6, 6, 7, 8, 9, 9, 10}, result)
}

// P-9
//
// node launch a deep mergesort with consensus
// many node are malicious, the merge sort will fail and an error is expected
func Test_PROJECT_deep_computation_with_consensus_many_malicious(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true), z.WithMaliciousNode(true))
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true), z.WithMaliciousNode(true))
	defer node4.Stop()

	node5 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true), z.WithMaliciousNode(true))
	defer node5.Stop()

	node6 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true), z.WithMaliciousNode(true))
	defer node6.Stop()

	node7 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMergeSortConsensus(true), z.WithMaliciousNode(true))
	defer node7.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node1.AddPeer(node4.GetAddr())
	node1.AddPeer(node5.GetAddr())
	node1.AddPeer(node6.GetAddr())
	node1.AddPeer(node7.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node2.AddPeer(node5.GetAddr())
	node2.AddPeer(node6.GetAddr())
	node2.AddPeer(node7.GetAddr())
	node3.AddPeer(node1.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node3.AddPeer(node4.GetAddr())
	node3.AddPeer(node5.GetAddr())
	node3.AddPeer(node6.GetAddr())
	node3.AddPeer(node7.GetAddr())
	node4.AddPeer(node1.GetAddr())
	node4.AddPeer(node2.GetAddr())
	node4.AddPeer(node3.GetAddr())
	node4.AddPeer(node5.GetAddr())
	node4.AddPeer(node6.GetAddr())
	node4.AddPeer(node7.GetAddr())
	node5.AddPeer(node1.GetAddr())
	node5.AddPeer(node2.GetAddr())
	node5.AddPeer(node3.GetAddr())
	node5.AddPeer(node4.GetAddr())
	node5.AddPeer(node6.GetAddr())
	node5.AddPeer(node7.GetAddr())
	node6.AddPeer(node1.GetAddr())
	node6.AddPeer(node2.GetAddr())
	node6.AddPeer(node3.GetAddr())
	node6.AddPeer(node4.GetAddr())
	node6.AddPeer(node5.GetAddr())
	node6.AddPeer(node7.GetAddr())
	node7.AddPeer(node1.GetAddr())
	node7.AddPeer(node2.GetAddr())
	node7.AddPeer(node3.GetAddr())
	node7.AddPeer(node4.GetAddr())
	node7.AddPeer(node5.GetAddr())
	node7.AddPeer(node6.GetAddr())

	time.Sleep(time.Millisecond * 1000)

	err, result := node1.MergeSort([]int{3, 2, 2, 5, 1, 3, 4})

	require.Error(t, err)
	require.NotEqual(t, []int{1, 2, 2, 3, 3, 4, 5}, result)
}

// P-10
//
//	map reduce beginning
func Test_PROJECT_mr_all_letters(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node4.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node1.AddPeer(node4.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node3.AddPeer(node1.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node3.AddPeer(node4.GetAddr())
	node4.AddPeer(node1.GetAddr())
	node4.AddPeer(node2.GetAddr())
	node4.AddPeer(node3.GetAddr())

	data := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}

	err, res := node1.MapReduce(3, data)
	require.Equal(t, nil, err)
	require.Equal(t, 26, len(res))
	for _, value := range res {
		require.Equal(t, 1, value)
	}

}

// P-11
//
//	test for the parser function
func Test_PROJECT_parser(t *testing.T) {
	filePath := "dataTest/P7.txt"

	data, err := impl.Parser(filePath)
	require.Equal(t, err, nil)
	require.Equal(t, []string{"this", "file", "is", "a", "test", "for", "the", "parser", "function"}, data)

}

// P-12
//
//	map reduce test to count word which appears multiple time
func Test_PROJECT_counter_test(t *testing.T) {
	transp := channel.NewTransport()
	filePath := "dataTest/P8.txt"

	nodes := CreateCompleteNetwork(5, t, transp)

	data, err := impl.Parser(filePath)
	require.Equal(t, err, nil)

	err, res := nodes[0].MapReduce(4, data)
	require.Equal(t, nil, err)
	printMap(res)
	require.Equal(t, 5, len(res))

}

// -----------------------------------------------------------------------------
// Utility functions

func CreateCompleteNetwork(nbNodes int, t *testing.T, transp transport.Transport) (listNodes []z.TestNode) {
	for k := 0; k < nbNodes; k++ {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		listNodes = append(listNodes, node)
	}

	for _, node := range listNodes {
		for _, neighbor := range listNodes {
			if neighbor.GetAddr() != node.GetAddr() {
				node.AddPeer(neighbor.GetAddr())
			}
		}
	}
	return listNodes
}

func printMap(m map[string]int) {
	println("┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓")
	for key, value := range m {
		fmt.Printf("┃ %20s ┃ %5s ┃\n", key, strconv.Itoa(value))
		println("┠━━━━━━━━━━━━━━━━━━━━━━╋━━━━━━━┫")
	}
}
