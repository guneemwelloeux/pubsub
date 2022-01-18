package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
)

func pullMsgById(w io.Writer, projectID, subID string, id string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(subID)
	sub.ReceiveSettings.NumGoroutines = 1
	sub.ReceiveSettings.MaxOutstandingMessages = 1
	fmt.Println(sub.ReceiveSettings)
	cctx, cancel := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		if msg.ID == id {
			fmt.Fprintf(w, "Message: %s %q\n", string(msg.ID), string(msg.Data))
			cancel()
			fmt.Println("cancelled")
		}
		msg.Nack()
	})
	if err != nil {
		return fmt.Errorf("Receive: %v", err)
	}
	fmt.Println("Done pulling 1 msg")
	return nil
}

func pullMsgs(w io.Writer, projectID, subID string, limit int) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	// Consume <limit> messages.
	//var mu sync.Mutex // For writing to the file, and modifying "received"
	received := make(chan int, 1)
	received <- 0
	sub := client.Subscription(subID)
	cctx, cancel := context.WithCancel(ctx)
	sub.ReceiveSettings.NumGoroutines = 1
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		//mu.Lock()
		//defer mu.Unlock()
		fmt.Fprintf(w, "Got message: %s %q\n", string(msg.ID), string(msg.Data))
		msg.Nack()
		r := 1 + (<-received)
		if r >= limit {
			cancel()
		}
		received <- r
	})
	if err != nil {
		return fmt.Errorf("Receive: %v", err)
	}
	return nil
}

func publishMsg(w io.Writer, projectID, topicID string, data []byte) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	var totalErrors uint64
	t := client.Topic(topicID)
	exists, _ := t.Exists(ctx)
	if !exists {
		t, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			log.Fatalln(err)
		}
	}

	result := t.Publish(ctx, &pubsub.Message{Data: data})

	wg.Add(1)
	go func(res *pubsub.PublishResult) {
		defer wg.Done()
		// The Get method blocks until a server-generated ID or
		// an error is returned for the published message.
		id, err := res.Get(ctx)
		if err != nil {
			// Error handling code can be added here.
			fmt.Fprintf(w, "Failed to publish: %v", err)
			atomic.AddUint64(&totalErrors, 1)
			return
		}
		fmt.Fprintf(w, "Published message, ID: %v\n", id)
	}(result)

	wg.Wait()

	if totalErrors > 0 {
		return fmt.Errorf("%d messages did not publish successfully", totalErrors)
	}
	return nil
}

func createSub(w io.Writer, projectID, topicID string, subID string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	topic := client.Topic(topicID)
	sub := client.Subscription(subID)
	exists, _ := sub.Exists(ctx)
	if !exists {
		_, err = client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			return err
		}
		fmt.Printf("Subscription %s/%s created\n", topicID, subID)
	} else {
		fmt.Println("Subscription already exists", subID)
	}
	return nil
}

func createTopic(w io.Writer, projectID, topicID string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	topic := client.Topic(topicID)
	exists, _ := topic.Exists(ctx)
	if !exists {
		topic, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			return err
		}
		fmt.Printf("Topic %s created\n", topicID)
	} else {
		fmt.Println("Topic already exists", topicID)
	}
	return nil
}

func main() {

	allCmd := flag.NewFlagSet("all", flag.ExitOnError)
	projectFlag := allCmd.String("p", "", "Project")

	publishCmd := flag.NewFlagSet("pub", flag.ExitOnError)
	topicFlag := publishCmd.String("t", "", "the topic to publish the message to")
	dataFlag := publishCmd.String("d", "", "the contents of the message. Use @<FILE> to read contents from FILE")

	pullCmd := flag.NewFlagSet("pull", flag.ExitOnError)
	subFlag := pullCmd.String("s", "", "the subscription to pull from")
	idFlag := pullCmd.String("i", "", "the id of the message to pull")
	countFlag := pullCmd.Int("n", 10, "the number of messages to pull")

	createSubCmd := flag.NewFlagSet("createsub", flag.ExitOnError)
	subNameFlag := createSubCmd.String("s", "", "the subscription to create")
	topicNameFlag := createSubCmd.String("t", "", "the topic to create the sub for")

	// Parse project flag if present
	allCmd.Parse(os.Args[1:])
	fmt.Println("Parsed", allCmd.Args())

	// Switch on subcommand
	switch allCmd.Args()[0] {
	case "pub":
		publishCmd.Parse(allCmd.Args()[1:])
		var r []byte
		if (*dataFlag)[0] == '@' {
			// open file
			var err error
			r, err = ioutil.ReadFile((*dataFlag)[1:])
			if err != nil {
				log.Fatalln(err)
			}
		} else {
			r = []byte(*dataFlag)
		}
		publishMsg(os.Stdout, *projectFlag, *topicFlag, r)

	case "pull":
		pullCmd.Parse(allCmd.Args()[1:])
		if *idFlag != "" {
			fmt.Println("Pull msg", *idFlag, "from", *projectFlag, "/", *subFlag)
			pullMsgById(os.Stdout, *projectFlag, *subFlag, *idFlag)
		} else {
			fmt.Println("Pull", *countFlag, "msg(s)", "from", *projectFlag, "/", *subFlag)
			pullMsgs(os.Stdout, *projectFlag, *subFlag, *countFlag)
		}

	case "createsub":
		createSubCmd.Parse(allCmd.Args()[1:])
		createSub(os.Stdout, *projectFlag, *topicNameFlag, *subNameFlag)
	case "createtopic":
		createSubCmd.Parse(allCmd.Args()[1:])
		createTopic(os.Stdout, *projectFlag, *topicNameFlag)

	}

}
