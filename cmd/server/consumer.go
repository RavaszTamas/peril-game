package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handleLog() func(log routing.GameLog) pubsub.Acktype {
	return func(log routing.GameLog) pubsub.Acktype {
		defer fmt.Printf("User {%s} - {%v}: %s", log.Username, log.CurrentTime, log.Message)
		if err := gamelogic.WriteLog(log); err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
