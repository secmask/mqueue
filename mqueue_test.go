package mqueue

import (
	"log"
	"testing"
	"time"
)

func TestInitMQueue(t *testing.T) {
	var m MQueue = make([]byte, 1024)
	InitMQueue(m)

	err := m.Put([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log(m)

	buff := make([]byte, 1024)
	n, err := m.Get(buff)
	if err != nil {
		t.Fatal(err)
	}
	if string(buff[:n]) != "hello" {
		t.Fatalf("Unexpected content %v[%d]", buff[:n], n)
	}

	for {
		if err = m.Put([]byte(time.Now().String())); err != nil {
			if err == ErrNoSpace {
				break
			} else {
				t.Fatal(err)
			}
		}
	}
	log.Printf("%d\n", m.Len())
	for i := 0; ; i++ {
		if n, err = m.Get(buff); err == nil {
			t.Logf("%d. %s\n", i, string(buff[:n]))
		}else {
			break
		}
	}
}
