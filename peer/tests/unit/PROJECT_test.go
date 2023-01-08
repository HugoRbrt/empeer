package unit

import (
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"strconv"
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

// P-5
//
// test for splitList function
func Test_PROJECT_splitList(t *testing.T) {
	transp := channel.NewTransport()

	funcTest := func(list []string, nb int) [][]string {
		var chunks [][]string
		size := len(list) / nb
		if len(list)%nb != 0 {
			size = size + 1
		}
		for i := 0; i < len(list); i += size {
			end := i + size
			// necessary check to avoid slicing beyond
			// slice capacity
			if end > len(list) {
				end = len(list)
			}
			chunks = append(chunks, list[i:end])
		}

		return chunks
	}

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()
	data := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}

	result := funcTest(data, 5)
	require.Equal(t, len(result), 5)
	require.Equal(t, result[0], []string{"1", "2"})
	require.Equal(t, result[1], []string{"3", "4"})
	require.Equal(t, result[4], []string{"9", "10"})

	result = funcTest(data, 3)
	require.Equal(t, len(result), 3)
	require.Equal(t, result[0], []string{"1", "2", "3", "4"})
	require.Equal(t, result[1], []string{"5", "6", "7", "8"})
	require.Equal(t, result[2], []string{"9", "10"})

}

// P-6
//
//	map reduce beginning
//
// ┌───► B
// A───► C
func Test_PROJECT_mrSplitSend(t *testing.T) {
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

	err := node1.MapReduce(3, data)
	require.Equal(t, err, nil)

}

// P-6
//
//	map reduce beginning
//
// ┌───► B
// A───► C
func Test_PROJECT_map(t *testing.T) {
	toMap := func(nbMapper int, data []string) (error, []map[string]int) {
		listMaps := make([]map[string]int, nbMapper)
		var keys []string
		// 97 is the value for 'a', 26 is the number of letters in the alphabet
		widthReducer := 27 / nbMapper
		if 27%nbMapper != 0 {
			widthReducer += 1
		}
		log.Info().Msgf("width: " + strconv.Itoa(widthReducer))
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
		return nil, listMaps
	}

	data := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}

	err, result := toMap(15, data)
	for nb, r := range result {
		log.Info().Msgf("MAPPER n°" + strconv.Itoa(nb))
		for key, _ := range r {
			log.Info().Msgf(key + ": " + strconv.Itoa(r[key]))
		}
	}
	require.Equal(t, err, nil)

}
