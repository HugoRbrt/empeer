package unit

import (
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"testing"
)

// P-1
func Test_PROJECT_local_computation(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	err, result := node1.MergeSort([]int{3, 2, 2, 5})
	require.Equal(t, err, nil)
	require.Equal(t, result, []int{2, 2, 3, 5})
}

// P-2
func Test_PROJECT_shallow_computation(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	println(node1.GetRoutingTable().String())

	err, result := node1.MergeSort([]int{3, 2, 2, 5, 1, 3, 4})
	require.Equal(t, err, nil)
	require.Equal(t, result, []int{1, 2, 2, 3, 3, 4, 5})
}
