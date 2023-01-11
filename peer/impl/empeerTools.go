package impl

import (
	"crypto/rsa"
	"sync"
)

// Notification
type NotificationEmpeerData struct {
	arr       []int
	signature []byte
	ip        string
	hash      []byte
	pk        *rsa.PublicKey
}

// NotificationEmpeer notify which sorted data has been received by whom
type NotificationEmpeer struct {
	// notif create for each PacketID which need an ack a channel for signaling if ack has been received or not
	notif map[string]chan []NotificationEmpeerData
	mu    sync.RWMutex
}

// Init initialize Notification
func (ne *NotificationEmpeer) Init() {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	ne.notif = make(map[string]chan []NotificationEmpeerData)
}

// requestNotif request for an Ack with the ID pckID
func (ne *NotificationEmpeer) requestNotif(pckID string) {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	ne.notif[pckID] = make(chan []NotificationEmpeerData, 1)
}

// deleteNotif delete the channel corresponding to the pckID
func (ne *NotificationEmpeer) deleteNotif(pckID string) {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	delete(ne.notif, pckID)
}

// waitNotif return a channel which is closed when ack has been received
func (ne *NotificationEmpeer) waitNotif(pckID string) chan []NotificationEmpeerData {
	ne.mu.Lock()
	defer ne.mu.Unlock()
	channel := ne.notif[pckID]
	return channel
}

// signalNotif signal by its corresponding channel that the pckID's Ack was received and its content
func (ne *NotificationEmpeer) signalNotif(pckID string, value []NotificationEmpeerData) {
	ne.mu.Lock()
	channel := ne.notif[pckID]
	ne.mu.Unlock()
	channel <- value
}
