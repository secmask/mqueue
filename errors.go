package mqueue

import (
	"errors"
)

var (
	ErrNoSpace        = errors.New("No space left")
	ErrPacketTooLarge = errors.New("Packet too large")
	ErrEmpty          = errors.New("Empty")
)
