package mqueue

import (
	"encoding/binary"
	"errors"
)

type MQueue []byte

const (
	hCapacity   = iota
	hReadPos    = iota * 8
	hWritePos   = iota * 8
	hReadCount  = iota * 8
	hWriteCount = iota * 8
	headerSize  = iota * 8
)

const (
	prefixSize       uint64 = 2 // we encode an element length with 2 bytes
	MaxElementLength uint16 = (1 << 16) - 2
)

func InitMQueue(m MQueue) error {
	if len(m) < headerSize {
		return errors.New("provide capacity too small")
	}
	m.setCapacity(uint64(len(m)))
	m.reset()
	return nil
}

func (m MQueue) reset() {
	m.setReadCount(0)
	m.setWriteCount(0)
	m.setReadPosition(headerSize)
	m.setWritePosition(headerSize)
}

func (m MQueue) ReadPosition() uint64 {
	return binary.LittleEndian.Uint64(m[hReadPos:])
}

func (m MQueue) WritePosition() uint64 {
	return binary.LittleEndian.Uint64(m[hWritePos:])
}

func (m MQueue) ReadCount() uint64 {
	return binary.LittleEndian.Uint64(m[hReadCount:])
}

func (m MQueue) WriteCount() uint64 {
	return binary.LittleEndian.Uint64(m[hWriteCount:])
}

func (m MQueue) Capacity() uint64 {
	return binary.LittleEndian.Uint64(m[hCapacity:])
}

func (m MQueue) setCapacity(v uint64) {
	binary.LittleEndian.PutUint64(m[hCapacity:], v)
}

func (m MQueue) setReadPosition(v uint64) {
	binary.LittleEndian.PutUint64(m[hReadPos:], v)
}

func (m MQueue) setWritePosition(v uint64) {
	binary.LittleEndian.PutUint64(m[hWritePos:], v)
}

func (m MQueue) setReadCount(v uint64) {
	binary.LittleEndian.PutUint64(m[hReadCount:], v)
}

func (m MQueue) setWriteCount(v uint64) {
	binary.LittleEndian.PutUint64(m[hWriteCount:], v)
}

func (m MQueue) Put(data []byte) error {
	pLen := len(data)
	if pLen > int(MaxElementLength) {
		return ErrPacketTooLarge
	}
	cap := m.Capacity()
	writePos := m.WritePosition()
	if (writePos + uint64(len(data)) + prefixSize) > cap {
		return ErrNoSpace
	}
	binary.LittleEndian.PutUint16(m[writePos:], uint16(pLen))
	writePos += prefixSize
	copy(m[writePos:], data)
	writePos += uint64(pLen)
	m.setWritePosition(writePos)
	m.setWriteCount(m.WriteCount() + 1)
	return nil
}

func (m MQueue) Len() uint64 {
	return m.WriteCount() - m.ReadCount()
}

func (m MQueue) Get(buff []byte) (n int, err error) {
	writeCount := m.WriteCount()
	readCount := m.ReadCount()
	if writeCount-readCount == 0 {
		err = ErrEmpty
		return
	}
	readPtr := m.ReadPosition()
	blockLength := binary.LittleEndian.Uint16(m[readPtr:])
	readPtr += prefixSize
	n = copy(buff, m[readPtr:readPtr+uint64(blockLength)])
	readCount++
	if readCount == writeCount {
		m.reset()
	} else {
		m.setReadPosition(readPtr + uint64(blockLength))
		m.setReadCount(readCount)
	}
	return
}

func (m MQueue) ReadableBytes() uint64 {
	return m.WritePosition() - m.ReadPosition()
}

func (m MQueue) freeSpace() uint64 {
	return m.Capacity() - m.WritePosition()
}

func (m MQueue) WriteTo(other MQueue) error {
	transferBytes := m.ReadableBytes()
	if other.freeSpace() < transferBytes {
		return ErrNoSpace
	}

	readPos := m.ReadPosition()
	copy(other[other.WritePosition():], m[readPos:readPos+transferBytes])
	other.setWritePosition(other.WritePosition() + transferBytes)
	other.setWriteCount(other.WriteCount() + m.Len())

	m.reset()
	return nil
}
