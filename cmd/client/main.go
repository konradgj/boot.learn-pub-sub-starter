package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril server...")
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not dial connection: %v", err)
	}
	defer connection.Close()
	fmt.Println("Successfully created a connection.")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("could not get username")
	}

	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("could not bind queue: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

LOOP:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				log.Printf("could not spawn: %v", err)
			}
		case "move":
			_, err := gameState.CommandMove(input)
			if err != nil {
				log.Printf("could not move: %v", err)
				break
			}
			fmt.Printf("Movement successful")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			break LOOP
		default:
			log.Println("Invalid command...")

		}
	}

	fmt.Println("\nClosing connection...")
	fmt.Println("Shutting down...")
}
