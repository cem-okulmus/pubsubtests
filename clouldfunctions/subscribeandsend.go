//  A simple test project to mess around a bit with Google Cloud authentication
//  and PubSub specifically
package cloudfunctions

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// WorkerPubSub replies to a request
func WorkerPubSub(ctx context.Context, m PubSubMessage) error {
	name := string(m.Data) // Automatically decoded from base64.
	if name == "" {
		return nil
	}

	// Sets your Google Cloud Platform project ID.
	projectID := "hgtest-1"

	// Creates a client.

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topic := client.Topic(name) // try to create topic

	topic.Publish(ctx, &pubsub.Message{
		Data: []byte("Request received"),
	})

	return nil
}

func main() {

	ctx := context.Background()

	// Sets your Google Cloud Platform project ID.
	projectID := "hgtest-1"

	fmt.Println("Creating a client.")

	// Creates a client.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// defer client.Close()

	fmt.Println("Client created.")

	// Sets the id for the new topic.
	topicID := "workerTopic"
	topic := client.Topic(topicID)

	topicID2 := "answerTopic"
	topic2, err := client.CreateTopic(ctx, topicID2)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	// fmt.Println("hooking to existing subscription")

	// sub := client.Subscription("subscription1")
	// if err != nil {
	// 	log.Println(err)
	// }

	// fmt.Println("attempting to create a subscription")

	// sub, err := client.CreateSubscription(context.Background(), "subscription1",
	// 	pubsub.SubscriptionConfig{Topic: topic})
	// if err != nil {
	// 	log.Println(err)
	// }

	// fmt.Println("subscription created!")

	fmt.Println("writing to topic")

	// Publish "hello world" on topic1.
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte("topicID2"),
	})

	fmt.Println("message published")

	// The publish happens asynchronously.
	// Later, you can get the result from res:

	_, err = res.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// // Use a callback to receive messages via subscription1.

	// fmt.Println("reading from sub")
	// err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	// 	fmt.Println(string(m.Data))
	// 	m.Ack()        // Acknowledge that we've consumed the message.
	// 	client.Close() // tostop receiving after the first
	// })
	// if err != nil {
	// 	log.Println(err)
	// }

	fmt.Println("Read something from sub")

	sub, err := client.CreateSubscription(context.Background(), "subscription1",
		pubsub.SubscriptionConfig{Topic: topic2})
	if err != nil {
		log.Println(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {

		fmt.Printf("Got message: %q\n", string(msg.Data))

		msg.Ack()

		cancel() // cancel right after receiving the first message

	})

	if err != nil {
		log.Fatalf("Issue with receiving: %v", err)
	}

	// clean up
	if err = sub.Delete(ctx); err != nil {
		log.Println(err)
	}

	if err := topic2.Delete(ctx); err != nil {
		log.Fatalf("Issue with deleting the topic: %v", err)
	}

	// fmt.Println("Deleted the subscription again.")

}
