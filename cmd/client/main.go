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

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("could not create channel on connection: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("could not get username")
	}

	userPauseQ := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		userPauseQ,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("could not bind queue %s: %v", userPauseQ, err)
	}

	userMovesQ := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilTopic,
		userMovesQ,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("could not bind to queue %s: %v", userMovesQ, err)
	}

	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		userPauseQ,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("could not subribe to queue %s: %v", userPauseQ, err)
	}

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		userMovesQ,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatalf("could not subribe to queue %s: %v", userPauseQ, err)
	}

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gameState),
	)

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
			mv, err := gameState.CommandMove(input)
			if err != nil {
				log.Printf("could not move: %v", err)
				break
			}

			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, userMovesQ, mv)
			if err != nil {
				log.Printf("could not publish move: %v", err)
			}

			fmt.Printf("Movement successful published\n")
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
