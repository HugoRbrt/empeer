package impl

import (
	"sync"
)

// Notification

// NotificationEmpeer notify which sorted data has been received by whom
type NotificationEmpeer struct {
	// notif create for each PacketID which need an ack a channel for signaling if ack has been received or not
	notif map[string]chan []int
	mu    sync.RWMutex
}

// Init initialize Notification
func (ne *NotificationEmpeer) Init() {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	ne.notif = make(map[string]chan []int)
}

// requestNotif request for an Ack with the ID pckID
func (ne *NotificationEmpeer) requestNotif(pckID string) {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	ne.notif[pckID] = make(chan []int, 1)
}

// waitNotif return a channel which is closed when ack has been received
func (ne *NotificationEmpeer) waitNotif(pckID string) chan []int {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	channel := ne.notif[pckID]
	return channel
}

// signalNotif signal by its corresponding channel that the pckID's Ack was received and its content
func (ne *NotificationEmpeer) signalNotif(pckID string, value []int) {
	ne.mu.Lock()
	channel := ne.notif[pckID]
	ne.mu.Unlock()
	channel <- value
}

// splitList split a given list into a list into nb uniform chunks
func (e *Empeer) splitList(list []string, nb int) [][]string {
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
