package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"os/signal"

	"github.com/barretodotcom/go-kafka/config"
	"github.com/barretodotcom/go-kafka/internal/handler"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	c := config.Load()
	addr, err := net.ResolveTCPAddr(c.Network, c.Address)
	if err != nil {
		panic(err)
	}
	listener, err := net.ListenTCP(c.Network, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	go handler.HandleConnections(listener, c)

	setGracefulShutdown()
}

func setGracefulShutdown() {
	n := make(chan os.Signal)

	signal.Notify(n, os.Interrupt, os.Kill)

	<-n
	fmt.Println("server is shutting down")
	time.Sleep(time.Millisecond * 500)
}
