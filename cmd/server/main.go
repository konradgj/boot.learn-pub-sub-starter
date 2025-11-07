package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nClosing connection...")
	fmt.Println("Shutting down...")
}
