package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	fmt.Println("Starting Peril client...")
	const connectionUrl = "amqp://guest:guest@localhost:5672/"

	username, err := gamelogic.ClientWelcome()

	if err != nil {
		log.Fatalf("Failed to read username: %v", err)
	}

	connection, err := amqp.Dial(connectionUrl)

	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	fmt.Println("Peril game client conencted to RabbitMQ")
	defer connection.Close()

	ch, queue, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.SimpleQueueTransient)

	if err != nil {
		log.Fatalf("Failed to declare and bind: %v", err)
	}

	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamestate := gamelogic.NewGameState(username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey)
		case "spawn":
			err := gamestate.CommandSpawn(words)
			if err != nil {
				fmt.Println("Units successfully spawned")
			} else {
				fmt.Println("Failed to move")
			}
		case "move":
			_, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Println("Successfull move")
			} else {
				fmt.Println("Failed to move")
			}
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet")
		default:
			fmt.Printf("Unkown command: %v\n", words[0])
		}
	}

	fmt.Println("Shuttig down!")

}
