package handler

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/barretodotcom/go-kafka/config"
	"github.com/barretodotcom/go-kafka/internal/model"
	"github.com/barretodotcom/go-kafka/kafka"
)

var consumers = make(map[string]kafka.Consumer)

func HandleConnections(listener *net.TCPListener, config *config.Config) {
	commands := make(chan model.Command)
	defer close(commands)
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			fmt.Printf("error when accepting connections: %s\n", err)
			continue
		}
		go readConnectionCommands(conn, commands)
		for i := 0; i <= config.Workers; i++ {
			go processCommands(config.Path, commands)
		}
	}
}

func readConnectionCommands(conn net.Conn, commands chan model.Command) {
	defer conn.Close()
	done := make(chan bool)
	reader := bufio.NewReader(conn)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			done <- true
			break
		}
		if err != nil {
			fmt.Printf("error while reading command: %s\n", err)
			continue
		}
		var command model.Command
		err = json.Unmarshal(line, &command)
		if err != nil {
			sendErrorToClient(conn, err)
			continue
		}
		command.Connection = conn
		commands <- command
	}
}

func processCommands(path string, commands <-chan model.Command) {
	for c := range commands {
		err := routeCommands(c, path)
		if err != nil {
			fmt.Printf("error during command routing: %s\n", err)
		}
	}
}

func routeCommands(command model.Command, path string) error {
	switch command.Type {
	case kafka.TypePublish:
		var message kafka.Message
		err := json.Unmarshal([]byte(command.Body), &message)
		if err != nil {
			return err
		}
		return kafka.Publish(command.Connection, message, command.Topic, path)
	case kafka.TypeConsume:
		consumer, err := kafka.NewConsumer(command.ConsumerName, command.Connection, command.Topic, path)
		if err != nil {
			return err
		}
		consumers[consumer.Name] = consumer
		go consumer.Start()
		return nil
	case kafka.TypeClose:
		for i := range consumers {
			if consumers[i].Conn == command.Connection {
				consumers[i].Close()
				delete(consumers, i)
			}
		}
		return nil
	}
	return nil
}

func sendErrorToClient(conn net.Conn, err error) {
	errString := fmt.Sprintf("%s", err)
	res, _ := json.Marshal(model.Response{Body: fmt.Sprintf(`{ "error": "%s" }`, errString), Type: kafka.TypeError})
	conn.Write(res)
}
