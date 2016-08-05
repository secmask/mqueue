package main

import (
	"fmt"
	"io/ioutil"
	"strconv"

	"gopkg.in/yaml.v2"
)

const (
	kilobyte = 1 << 10
	megabyte = 1 << 20
	gigabyte = 1 << 30
)

type Config struct {
	HostAndPort   string    `yaml:"host_port"`
	FileBlockUnit HumanSize `yaml:"file_block_unit"`
	Cache         HumanSize `yaml:"cache_size"`
	DataDir       string    `yaml:"data_dir"`
	LogTo         string    `yaml:"log_to"`
}

type HumanSize string

func (s HumanSize) Value() (int64, error) {
	return ParseHumanSize(string(s))
}

func (s HumanSize) ValueWithDefault(v int64) int64 {
	i, err := ParseHumanSize(string(s))
	if err == nil {
		return i
	}
	return v
}

func ConfigFromFile(f string) (*Config, error) {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}
	return ParseConfig(data)
}

func ParseConfig(data []byte) (*Config, error) {
	c := &Config{}
	err := yaml.Unmarshal(data, c)
	return c, err
}

func ParseHumanSize(s string) (int64, error) {
	sLen := len(s)
	if sLen == 0 {
		return 0, fmt.Errorf("Not number [%s]", s)
	}
	lastChar := s[sLen-1]
	switch lastChar {
	case 'k':
		fallthrough
	case 'K':
		v, err := strconv.ParseInt(s[:sLen-1], 10, 0)
		return v * kilobyte, err
	case 'm':
		fallthrough
	case 'M':
		v, err := strconv.ParseInt(s[:sLen-1], 10, 0)
		return v * megabyte, err
	case 'g':
		fallthrough
	case 'G':
		v, err := strconv.ParseInt(s[:sLen-1], 10, 0)
		return v * gigabyte, err
	default:
		return strconv.ParseInt(s[:sLen], 10, 0)
	}
}
