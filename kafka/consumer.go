package kafka

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/barretodotcom/go-kafka/internal/model"
)

type Consumer struct {
	ID        string
	TopicFile *os.File
	Reader    *bufio.Reader
	Topic     string
	Conn      io.ReadWriteCloser

	Name     string
	MetaFile *os.File
	Meta     *model.MetaConsumer
	Done     chan bool
}

func NewConsumer(name string, conn net.Conn, topic string, path string) (Consumer, error) {
	consumerFilename := fmt.Sprintf("%s/%s.%s.consumer", path, topic, name)
	file, err := os.OpenFile(consumerFilename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return Consumer{}, fmt.Errorf("corrupted file: %s, error: %s", consumerFilename, err)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return Consumer{}, fmt.Errorf("corrupted file: %s, error: %s", consumerFilename, err)
	}
	if len(data) == 0 {
		data = []byte("{}")
	}

	var meta model.MetaConsumer
	err = json.Unmarshal(data, &meta)
	if err != nil {
		return Consumer{}, err
	}

	topicFilename := fmt.Sprintf("%s/%s.topic", path, topic)
	topicFile, err := os.OpenFile(topicFilename, os.O_RDWR|os.O_CREATE, 0755)

	reader := bufio.NewReader(topicFile)
	for i := 0; i < meta.Offset; i++ {
		reader.ReadLine()
	}

	done := make(chan bool)
	return Consumer{
		ID:        topicFilename,
		Reader:    reader,
		TopicFile: topicFile,
		Name:      name,
		MetaFile:  file,
		Meta:      &meta,
		Done:      done,
		Conn:      conn,
	}, err
}

func (c Consumer) Start() {
	fmt.Printf("%s consuming messages...\n", c.Name)
	for {
		select {
		case <-c.Done:
			return
		default:
			line, _, err := c.Reader.ReadLine()
			if err == io.EOF {
				time.Sleep(time.Second * 1)
				continue
			}

			if err != nil {
				fmt.Printf("%s unable to read topic file: %s", c.ID, err)
				continue
			}
			response := model.Response{
				Offset: uint(c.Meta.Offset),
				Body:   string(line),
			}

			resp, err := json.Marshal(response)
			if err != nil {
				fmt.Printf("%s unable to marshal response json: %s", c.ID, err)
				continue
			}
			c.Conn.Write(resp)
			c.Meta.Offset++
			c.updateMetaFile()
		}
	}
}

func (c Consumer) Close() {
	fmt.Printf("%s closing...\n", c.ID)
	close(c.Done)
	c.updateMetaFile()
	c.TopicFile.Close()
	c.MetaFile.Close()
	c.Conn.Close()
}

func (c Consumer) updateMetaFile() {
	data, _ := json.Marshal(c.Meta)

	err := c.MetaFile.Truncate(0)
	if err != nil {
		fmt.Printf("unable to update metaFile: %s\n", err)
		return
	}
	c.MetaFile.Seek(0, 0)
	_, err = c.MetaFile.Write(data)
	if err != nil {
		fmt.Printf("unable to update metaFile: %s\n", err)
	}
}
