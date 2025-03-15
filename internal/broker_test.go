package internal

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func TestBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	b := NewBroker[int]()
	go b.Start(context.TODO())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				b.Publish(rand.Intn(100))
			}
		}
	}()

	for ctx.Err() == nil {
		go func(dataC chan int) {
			defer b.Unsubscribe(dataC)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(rand.Intn(100)) * time.Millisecond):
				return
			}
		}(b.Subscribe())
	}
}
