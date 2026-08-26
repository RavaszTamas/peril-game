package gamelogic

import (
	"fmt"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (gs *GameState) CommandSpam(ch *amqp.Channel, words []string) error {
	if len(words) != 2 {
		return fmt.Errorf("usage: spam <number>")
	}

	spamCount, err := strconv.Atoi(words[1])

	if err != nil {
		return fmt.Errorf("usage: spam <number>\nnumber must be positive!")
	}

	for i := 0; i < spamCount; i++ {
		message := GetMaliciousLog()
		if err := pubsub.PublishGob(
			ch,
			routing.ExchangePerilTopic,
			routing.GameLogSlug+"."+gs.GetUsername(),
			routing.GameLog{
				CurrentTime: time.Now(),
				Message:     message,
				Username:    gs.GetUsername(),
			},
		); err != nil {
			return err
		}
	}

	return nil
}
