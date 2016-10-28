package mqueue

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/edsrzf/mmap-go"
)

type CompositeQueueOption struct {
	Name          string
	CacheSize     uint64
	BackFile      string
	FileBlockUnit uint64
}

// CompositeQueue is combine of a memory queue and memory map queue,
// when the memory queue is full, it transfer to memory map queue
type CompositeQueue struct {
	cacheQueue     MQueue               // memory queue
	mapQueue       MQueue               // memory map file queue
	mapFile        mmap.MMap            // memory map file correspond to memory map queue
	option         CompositeQueueOption // options for this composite queue
	readFromFile   bool                 // if true, pop operation should be go with memory map queue
	backFileHandle *os.File             // file handle to memory map
	lock           sync.Locker          // lock guard to protect concurrent access to this composite queue
	dataChan       chan []byte          // a chan object help us implement "BRPOP" command.
	deleted        bool
}

func OpenCompositionQueue(option CompositeQueueOption) (*CompositeQueue, error) {
	m := &CompositeQueue{
		cacheQueue:   make([]byte, option.CacheSize),
		option:       option,
		readFromFile: false,
		lock:         &sync.Mutex{},
		dataChan:     make(chan []byte),
	}
	err := InitMQueue(m.cacheQueue)
	if err != nil {
		return nil, err
	}
	if err = m.mapBackFile(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *CompositeQueue) Chan() <-chan []byte {
	return m.dataChan
}

func (m *CompositeQueue) mapBackFile() (err error) {
	lf := log.Fields{
		"func":   "CompositeQueue#mapBackFile",
		"option": m.option,
	}
	m.backFileHandle, err = os.OpenFile(m.option.BackFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.WithFields(lf).WithError(err).Error("failed to open file")
		return
	}
	stat, err := m.backFileHandle.Stat()
	if err != nil {
		log.WithFields(lf).WithError(err).Error("failed to stat file")
		return err
	}
	loadSize := stat.Size()
	if loadSize == 0 {
		if err = m.backFileHandle.Truncate(int64(m.option.FileBlockUnit)); err != nil {
			log.WithFields(lf).WithError(err).Error("failed to truncate file")
			return err
		}
	}
	m.mapFile, err = mmap.Map(m.backFileHandle, mmap.RDWR, 0)
	if err != nil {
		return
	}

	m.mapQueue = []byte(m.mapFile)
	if loadSize == 0 {
		err = InitMQueue(m.mapQueue)
	} else {
		m.readFromFile = true
	}

	return
}

func (m *CompositeQueue) Get(buff []byte) (int, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.deleted {
		return 0, ErrEmpty
	}
	if m.readFromFile {
		n, err := m.mapQueue.Get(buff)
		if err == ErrEmpty {
			m.readFromFile = false
			return m.cacheQueue.Get(buff)
		}
		return n, err
	}
	return m.cacheQueue.Get(buff)
}

func (m *CompositeQueue) Put(data []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.deleted {
		return ErrNoSpace
	}
	if !m.readFromFile {
		select {
		case m.dataChan <- data:
			return nil
		default:
			break
		}
	}

	err := m.cacheQueue.Put(data)
	if err == nil {
		return nil
	}
	if err == ErrNoSpace {
		if err = m.transferToDisk(); err == nil {
			return m.cacheQueue.Put(data)
		}
	}
	return err
}

func (m *CompositeQueue) transferToDisk() (err error) {
	if m.mapQueue.freeSpace() < m.cacheQueue.ReadableBytes() {
		newSize := m.mapQueue.Capacity() + m.option.FileBlockUnit
		log.Printf("Try to expand %s to %d\n", m.option.BackFile, newSize)
		if err = m.backFileHandle.Truncate(int64(newSize)); err != nil {
			return
		}
		err = m.mapFile.Flush()
		err = m.mapFile.Unmap()
		m.mapFile, err = mmap.Map(m.backFileHandle, mmap.RDWR, 0)
		if err != nil {
			return
		}
		m.mapQueue = []byte(m.mapFile)
		m.mapQueue.setCapacity(newSize)
	}

	if err = m.cacheQueue.WriteTo(m.mapQueue); err == nil {
		m.readFromFile = true
	}
	return
}

func (m *CompositeQueue) Len() uint64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.deleted {
		return 0
	}
	if m.readFromFile {
		return m.cacheQueue.Len() + m.mapQueue.Len()
	}
	return m.cacheQueue.Len()
}

func (m *CompositeQueue) closeSink() error {

	err := m.transferToDisk()
	if err != nil {
		log.Printf("Failed to transfer to disk %s: %v\n", m.option.Name, err)
	}
	if err = m.mapFile.Unmap(); err != nil {
		log.Printf("Failed to Unmap %s: %v\n", m.option.Name, err)
	}
	if err = m.backFileHandle.Close(); err != nil {
		log.Printf("Failed to close file %s: %v\n", m.option.Name, err)
	}
	return err
}

func (m *CompositeQueue) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.closeSink()
}

func (m *CompositeQueue) Delete() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	lf := log.Fields{
		"func":     "CompositeQueue#Delete()",
		"backFile": m.option.BackFile,
		"name":     m.option.Name,
	}
	err := m.closeSink()
	if err != nil {
		log.WithFields(lf).WithError(err).Error("failed to close queue")
	}
	err = os.Remove(m.option.BackFile)
	if err != nil {
		log.WithFields(lf).WithError(err).Error("failed to delete file")
	}
	m.deleted = true
	m.mapQueue = nil
	return err
}
