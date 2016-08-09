package main

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	rp "github.com/secmask/go-redisproto"
	"github.com/secmask/mqueue"

	log "github.com/Sirupsen/logrus"
)

type Client struct {
	conn        net.Conn
	redisWriter *rp.Writer
	qMan        *QueueMan
	context     context.Context
	buffer      []byte
}

func NewClient(conn net.Conn, ctx context.Context, qMan *QueueMan) *Client {
	return &Client{
		conn:    conn,
		context: ctx,
		qMan:    qMan,
		buffer:  make([]byte, mqueue.MaxElementLength),
	}
}

func (c *Client) Run(wg *sync.WaitGroup) {
	defer c.conn.Close()
	defer wg.Done()

	parser := rp.NewParser(c.conn)
	c.redisWriter = rp.NewWriter(bufio.NewWriter(c.conn))
	commands := parser.Commands()
	for {
		select {
		case cmd, ok := <-commands:
			if !ok {
				return
			}
			c.processCommand(cmd)
		case <-c.context.Done():
			return
		}
	}
}

func (c *Client) processCommand(cmd *rp.Command) (err error) {
	action := strings.ToUpper(string(cmd.Get(0)))
	switch action {
	case "PING":
		err = c.redisWriter.WriteSimpleString("PONG")
	case "QUIT":
		err = c.redisWriter.WriteSimpleString("OK")
	case "LPUSH":
		err = c.handleLPUSH(cmd)
	case "RPOP":
		err = c.handleRPOP(cmd)
	case "BRPOP":
		err = c.handleBRPOP(cmd)
	case "LLEN":
		err = c.handleLLEN(cmd)
	case "KEYS":
		err = c.handleKEYS(cmd)
	case "DEL":
		err = c.handleDEL(cmd)
	default:
		err = c.redisWriter.WriteError("Unsupported command")
	}
	err = c.redisWriter.Flush()
	return
}

func (c *Client) handleDEL(cmd *rp.Command) error {
	qName := string(cmd.Get(1))
	err := c.qMan.Delete(qName)
	if err != nil {
		return c.redisWriter.WriteError(err.Error())
	}
	return c.redisWriter.WriteBulkString("OK")
}

func (c *Client) handleKEYS(cmd *rp.Command) error {
	queues := c.qMan.Queues()
	return c.redisWriter.WriteBulkStrings(queues)
}

func (c *Client) handleLLEN(cmd *rp.Command) error {
	qName := string(cmd.Get(1))
	q, err := c.qMan.GetOrCreate(qName)
	if err != nil {
		c.redisWriter.WriteError(err.Error())
		return err
	}
	return c.redisWriter.WriteInt(int64(q.Len()))
}

func (c *Client) handleRPOP(cmd *rp.Command) error {
	qName := string(cmd.Get(1))
	q, err := c.qMan.GetOrCreate(qName)
	if err != nil {
		c.redisWriter.WriteError(err.Error())
		return err
	}

	n, err := q.Get(c.buffer)
	if err != nil {
		if err == mqueue.ErrEmpty {
			return c.redisWriter.WriteBulk(nil)
		}
		lf := log.Fields{
			"func": "handleRPOP",
		}
		log.WithFields(lf).WithError(err).Error("Unexpected error")
		return c.redisWriter.WriteError(err.Error())
	}
	return c.redisWriter.WriteBulk(c.buffer[:n])
}

func (c *Client) handleBRPOP(cmd *rp.Command) error {
	qName := string(cmd.Get(1))
	timeout, err := strconv.Atoi(string(cmd.Get(2)))

	if err != nil {
		return c.redisWriter.WriteError(err.Error())
	}

	q, err := c.qMan.GetOrCreate(qName)
	if err != nil {
		c.redisWriter.WriteError(err.Error())
		return err
	}

	n, err := q.Get(c.buffer)
	if err == nil {
		return c.redisWriter.WriteBulk(c.buffer[:n])
	}
	if err != mqueue.ErrEmpty {
		lf := log.Fields{
			"func": "handleBRPOP",
		}
		log.WithFields(lf).WithError(err).Error("Unexpected error")
		return c.redisWriter.WriteError(err.Error())
	}
	select {
	case data := <-q.Chan():
		return c.redisWriter.WriteBulk(data)
	case <-time.After(time.Second * time.Duration(timeout)):
		return c.redisWriter.WriteBulk(nil)
	}
}

func (c *Client) handleLPUSH(cmd *rp.Command) error {
	qName := string(cmd.Get(1))
	data := cmd.Get(2)
	q, err := c.qMan.GetOrCreate(qName)
	if err != nil {
		return c.redisWriter.WriteError(err.Error())
	}
	err = q.Put(data)
	if err != nil {
		return c.redisWriter.WriteError(err.Error())
	}
	return c.redisWriter.WriteInt(1)
}
