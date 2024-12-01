package p2p

import "time"

type EventType uint8

const (
	EventTypeNodeNew EventType = iota
	EventTypeNodeDisconnected
	EventTypeNodeError
	EventTypeTransportError
	EventTypeHandshakeCompleted
	EventTypeHandshakeError
)

type Event struct {
	EventType  EventType
	Timestamp  time.Time
	RemoteNode Node // interface can be nil
	ResponseCh chan error
}

func NewEvent(eventType EventType, remoteNode Node) Event {
	return Event{
		EventType:  eventType,
		Timestamp:  time.Now(),
		RemoteNode: remoteNode,
		ResponseCh: make(chan error, 1), // Buffered channel to prevent deadlock
	}
}
