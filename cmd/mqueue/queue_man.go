package main

import (
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/secmask/mqueue"

	log "github.com/Sirupsen/logrus"
)

type QueueMan struct {
	queues    map[string]*mqueue.CompositeQueue
	protector sync.Locker
	conf      *Config
}

func NewQueueMan(conf *Config) *QueueMan {
	return &QueueMan{
		queues:    make(map[string]*mqueue.CompositeQueue),
		protector: &sync.Mutex{},
		conf:      conf,
	}
}

func (q *QueueMan) GetOrCreate(qName string) (*mqueue.CompositeQueue, error) {
	q.protector.Lock()
	defer q.protector.Unlock()
	m, ok := q.queues[qName]
	if ok {
		return m, nil
	}
	opt := mqueue.CompositeQueueOption{
		Name:          qName,
		BackFile:      path.Join(q.conf.DataDir, qName+".mq"),
		FileBlockUnit: uint64(q.conf.FileBlockUnit.ValueWithDefault(gigabyte)),
		CacheSize:     uint64(q.conf.Cache.ValueWithDefault(8 * megabyte)),
	}
	m, err := mqueue.OpenCompositionQueue(opt)
	if err != nil {
		return nil, err
	}
	q.queues[qName] = m
	return m, nil
}

func (q *QueueMan) Delete(qName string) error {
	q.protector.Lock()
	defer q.protector.Unlock()
	m, ok := q.queues[qName]
	if !ok {
		return nil
	}
	err := m.Delete()
	if err != nil {
		return err
	}
	delete(q.queues, qName)
	return nil
}

func (q *QueueMan) Queues() []string {
	q.protector.Lock()
	defer q.protector.Unlock()
	res := make([]string, 0, 8)
	for k := range q.queues {
		res = append(res, k)
	}
	return res
}

func (q *QueueMan) CloseAll() {
	lf := log.Fields{
		"func": "QueueMan#CloseAll",
	}
	q.protector.Lock()
	defer q.protector.Unlock()
	for k, m := range q.queues {
		if err := m.Close(); err == nil {
			delete(q.queues, k)
		} else {
			log.WithFields(lf).WithError(err).Errorf("failed to close queue %s", k)
		}
	}
}

func (q *QueueMan) Load() {
	lf := log.Fields{
		"func": "QueueMan#Load",
	}
	files, err := filepath.Glob(path.Join(q.conf.DataDir, "*.mq"))
	if err != nil {
		log.WithFields(lf).WithError(err).Error("failed to listing data file")
		return
	}
	for _, f := range files {
		baseName := filepath.Base(f)
		qName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
		opt := mqueue.CompositeQueueOption{
			Name:          qName,
			BackFile:      f,
			FileBlockUnit: uint64(q.conf.FileBlockUnit.ValueWithDefault(gigabyte)),
			CacheSize:     uint64(q.conf.Cache.ValueWithDefault(8 * megabyte)),
		}
		m, err := mqueue.OpenCompositionQueue(opt)
		if err != nil {
			log.WithFields(lf).WithError(err).Error("failed to load data file")
			continue
		}
		q.queues[qName] = m
	}
}
