package unit

import (
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"testing"
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

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())

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
	require.Equal(t, err, nil)
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
	require.Equal(t, err, nil)
	require.Equal(t, result, []int{-1, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 4, 5, 5, 6, 6, 7, 8, 9, 9, 10})
}
