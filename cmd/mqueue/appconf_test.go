package main

import "testing"

var (
	conf = []byte(`
host_port: localhost:1607
file_block_unit: 1g
cache_size: 10m
data_dir: ./data
`)

)

func TestParseConfig(t *testing.T) {
	c, err := ParseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := c.FileBlockUnit.Value(); err != nil || v != gigabyte {
		t.Fatalf("Unexpected file block %v, %v", v, err)
	}
	if v, err := c.Cache.Value(); err != nil || v != 10*megabyte {
		t.Fatalf("Unexpected cache size %v, %v", v, err)
	}
	if c.HostAndPort!="localhost:1607" {
		t.Errorf("Unexpected host_port %s",c.HostAndPort)
	}
}
