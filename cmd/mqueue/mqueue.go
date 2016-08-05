package main

import (
	"flag"

	"context"
	log "github.com/Sirupsen/logrus"
	"github.com/secmask/mqueue"
	"gopkg.in/natefinch/lumberjack.v2"
	"net"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
)

func main0() {
	opt := mqueue.CompositeQueueOption{
		FileBlockUnit: 1024,
		Name:          "k1",
		CacheSize:     128,
		BackFile:      "k1.sq",
	}
	q, err := mqueue.OpenCompositionQueue(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()
	log.Println("startup with length ", q.Len())
	data := []byte("hello")
	for i := 0; i < 1000; i++ {
		err = q.Put(data)
		log.Printf("%d. ", i)
		if err != nil {
			log.Println(err)
			break
		}
	}
	log.Println(q.Len())
}

var (
	configFile = flag.String("c", "config.yml", "config file")
)

type AppContext struct {
	context.Context
	done <-chan struct{}
}

func (c *AppContext) Done() <-chan struct{} {
	return c.done
}

func ctxWithDone(parent context.Context, done <-chan struct{}) *AppContext {
	return &AppContext{
		done:    done,
		Context: parent,
	}
}

func main() {
	flag.Parse()
	config, err := ConfigFromFile(*configFile)
	if err != nil {
		log.WithError(err).Error("failed to load config file")
	}

	if config.LogTo != "stdout" {
		if IsDirectory(config.LogTo) {
			ConfigLog(path.Join(config.LogTo, "app.log"), 20, 20, 30)
		} else {
			if err := os.Mkdir(config.LogTo, 0755); err == nil {
				ConfigLog(path.Join(config.LogTo, "app.log"), 20, 20, 30)
			}
		}
	}

	done := make(chan struct{})
	appContext := ctxWithDone(context.Background(), done)

	listener, err := net.Listen("tcp", config.HostAndPort)
	if err != nil {
		panic(err)
	}
	qMan := NewQueueMan(config)
	qMan.Load()
	wg := &sync.WaitGroup{}
	go func(){
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Error on accept: ", err)
				continue
			}
			client := NewClient(conn, appContext, qMan)
			wg.Add(1)
			go client.Run(wg)
		}
	}()

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, syscall.SIGINT, syscall.SIGTERM)
	for range osSignal {
		close(done)
		log.Println("wait for clean close all client")
		wg.Wait()
		break
	}
	qMan.CloseAll()
	log.Println("Shutdown safe")
}

func ConfigLog(fileName string, maxSizeinMB int, maxBackup int, maxAge int) {
	log.SetOutput(&lumberjack.Logger{
		Filename:   fileName,
		MaxSize:    maxSizeinMB, // megabytes
		MaxBackups: maxBackup,
		MaxAge:     maxAge, //days
	})
}

func IsDirectory(path string) bool {
	if fileInfo, err := os.Stat(path); err != nil {
		return false
	} else {
		return fileInfo.IsDir()
	}
}
