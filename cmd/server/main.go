package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connectionUrl = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	connection, err := amqp.Dial(connectionUrl)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer connection.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	<-sigs

	fmt.Println("Shuttig down!")
}
