package mqueue

import (
	"testing"
)

func TestOpenCompositionQueue(t *testing.T) {
	opt := CompositeQueueOption{
		FileBlockUnit: 1024,
		Name:          "k1",
		CacheSize:     128,
		BackFile:      "k1.sq",
	}
	q, err := OpenCompositionQueue(opt)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello")
	for i := 0; i < 200; i++ {
		err = q.Put(data)
		t.Logf("%d. ", i)
		if err != nil {
			t.Log(err)
			break
		}
	}
}
