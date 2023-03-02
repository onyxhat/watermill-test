// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/goombaio/namegenerator"
)

func main() {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		watermill.NewStdLogger(false, false),
	)

	messages, err := pubSub.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publishMessages(pubSub)
}

func publishMessages(publisher message.Publisher) {
	for {
		id := watermill.NewUUID()
		name := randomName()

		fmt.Printf("sending message: %s, payload: %s\n", id, name)

		msg := message.NewMessage(id, []byte(name))

		if err := publisher.Publish("example.topic", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func randomName() string {
	seed := time.Now().UTC().UnixNano()
	nameGenerator := namegenerator.NewNameGenerator(seed)

	return nameGenerator.Generate()
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		fmt.Printf("received message: %s, payload: %s\n", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
