package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/websocket"

	"github.com/c-bata/go-prompt"
)

func main() {
	if err := do(); err != nil {
		log.Fatal(err)
	}

}

var queuePrefix string
var serverURL string

func init() {
	flag.StringVar(&queuePrefix, "queue-prefix", "", "HTTP Prefix for SQS queues")
	flag.StringVar(&serverURL, "server-url", "", "The URL to POST to for a sync handler")

}

var client = &http.Client{
	Timeout: time.Second * 120,
}

type Controller struct {
	queues       map[string]*workerQueue
	client       *sqs.SQS
	queuePrompts []prompt.Suggest
}

type workerQueue struct {
	liveQueue *sqsQueue
	deadQueue *sqsQueue
	name      string
}

var rootCommands = []prompt.Suggest{
	{Text: "exit", Description: "End the program"},
	{Text: "ls", Description: "List the dead letter queues"},
	{Text: "requeue", Description: "Re queue all messages on the dead letter to the main queue"},
	{Text: "interact", Description: "Manually work through the dead messages"},
}

func (c Controller) rootCompleter(d prompt.Document) []prompt.Suggest {
	args := strings.Split(d.TextBeforeCursor(), " ")

	if len(args) <= 1 {
		return prompt.FilterHasPrefix(rootCommands, args[0], true)
	}
	if len(args) != 2 {
		return []prompt.Suggest{}
	}

	switch args[0] {
	case "requeue", "interact":
		return prompt.FilterHasPrefix(c.queuePrompts, args[1], true)
	default:
		return []prompt.Suggest{}
	}
}

func (c *Controller) RootLoop() error {

	for q := range c.queues {
		c.queuePrompts = append(c.queuePrompts, prompt.Suggest{
			Text:        q,
			Description: "Queue " + q,
		})
	}

	for {
		cmd := strings.Split(prompt.Input("> ", c.rootCompleter), " ")
		switch cmd[0] {
		case "exit":
			return nil
		case "ls":
			c.ShowDead()
			continue

		}
		if len(cmd) != 2 {
			fmt.Printf("Invalid Command\n")
			continue
		}

		switch cmd[0] {
		case "requeue":
			if err := c.Requeue(cmd[1]); err != nil {
				fmt.Printf("Error: %s\nEnd Command\n", err.Error())
			}
		case "interact":
			if err := c.Interact(cmd[1]); err != nil {
				fmt.Printf("Error: %s\nEnd Command\n", err.Error())
			}
		default:
			fmt.Printf("Invalid Command\n")
		}

	}
	return nil
}

func (c Controller) ShowDead() {
	fmt.Println("Dead Letters:")
	for _, queue := range c.queues {
		len, err := queue.deadQueue.Length()
		if err != nil {
			panic("Can't list " + queue.name + ": " + err.Error())
		}
		fmt.Printf("%20s - %d\n", queue.name, len)
	}
}

func (c Controller) Requeue(name string) error {

	queue, ok := c.queues[name]
	if !ok {
		return fmt.Errorf("No such queue")
	}

	for {
		output, err := queue.deadQueue.client.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queue.deadQueue.url),
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64(1),
		})

		if err != nil {
			return err
		}
		if len(output.Messages) == 0 {
			return nil
		}

		batch := make([]*sqs.SendMessageBatchRequestEntry, 0, len(output.Messages))
		for _, msg := range output.Messages {
			batch = append(batch, &sqs.SendMessageBatchRequestEntry{
				Id:          msg.MessageId,
				MessageBody: msg.Body,
			})
		}
		sendResp, err := queue.liveQueue.client.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queue.liveQueue.url),
			Entries:  batch,
		})

		if err != nil {
			return err
		}
		if len(sendResp.Failed) > 0 {
			return fmt.Errorf("Send Resp Failed")
		}
		for _, entry := range sendResp.Successful {
			fmt.Printf("Requeue %s as %s\n", aws.StringValue(entry.Id), aws.StringValue(entry.MessageId))
		}

		deleteBatch := make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(output.Messages))
		for _, msg := range output.Messages {
			deleteBatch = append(deleteBatch, &sqs.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			})
		}
		deleteResp, err := queue.deadQueue.client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(queue.deadQueue.url),
			Entries:  deleteBatch,
		})
		if err != nil {
			return err
		}
		if len(deleteResp.Failed) > 0 {
			return fmt.Errorf("Delete Failed")
		}

	}

}

func (c Controller) interactCompleter(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "post", Description: "Run the message on the server, and come back to it"},
		{Text: "delete", Description: "Delete the message from the dead queue"},
		{Text: "requeue", Description: "Place the message back on the main queue"},
		{Text: "skip", Description: "Leave on the dead queue, setting a 5 minute visibility timeout"},
		{Text: "exit", Description: "Back to main"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func (c Controller) Interact(name string) error {
	q, ok := c.queues[name]
	if !ok {
		return fmt.Errorf("No such queue")
	}

	for {
		msg, err := q.deadQueue.Next()
		if err != nil {
			return err
		}

		fmt.Printf("\nMessage %s\n", aws.StringValue(msg.message.MessageId))
		bodyBytes := []byte(aws.StringValue(msg.message.Body))
		buff := bytes.NewBuffer([]byte{})
		json.Indent(buff, bodyBytes, "", "  ")
		fmt.Println(buff.String())

	commandLoop:
		for {
			cmd := prompt.Input("MSG> ", c.interactCompleter)
			switch cmd {
			case "exit":
				return nil
			case "requeue":
				if id, err := q.liveQueue.Publish(bodyBytes); err != nil {
					return err
				} else {
					fmt.Printf("Requeued as %s\n", id)
					msg.Ack()
				}
				break commandLoop

			case "post":
				fmt.Println("Running POST to live server")
				if err := postMessage(bodyBytes, name); err != nil {
					return err
				}
				continue commandLoop

			case "delete":
				msg.Ack()
				break commandLoop
			case "skip":
				msg.KeepAlive(time.Minute * 5)
				break commandLoop
			default:
				fmt.Println("Invalid Command")
				continue commandLoop
			}
		}

		return nil

	}
}

func LoadQueues(service *sqs.SQS, namePrefix string) (map[string]*workerQueue, error) {

	out, err := service.ListQueues(&sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(namePrefix),
	})
	if err != nil {
		return nil, err
	}

	queueSet := map[string]*sqsQueue{}
	queueDeadSet := map[string]*sqsQueue{}

	for _, ptr := range out.QueueUrls {
		urlString := aws.StringValue(ptr)
		urlParsed, err := url.Parse(urlString)
		if err != nil {
			return nil, err
		}
		_, name := path.Split(urlParsed.Path)
		name = strings.TrimPrefix(name, namePrefix)
		thisQueue := &sqsQueue{
			client: service,
			url:    urlString,
			name:   name,
		}

		if strings.HasSuffix(name, "-dead") {
			queueDeadSet[name] = thisQueue
		} else {
			queueSet[name] = thisQueue
		}
	}

	allQueues := map[string]*workerQueue{}

	for name, live := range queueSet {
		dead, ok := queueDeadSet[name+"-dead"]
		if !ok {
			fmt.Printf("Warning: Queue %s has no dead letter, excluding\n", name)
			continue
		}

		allQueues[name] = &workerQueue{
			liveQueue: live,
			deadQueue: dead,
			name:      name,
		}
	}

	return allQueues, nil

}

func do() error {

	flag.Parse()

	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	service := sqs.New(sess, &aws.Config{})

	allQueues, err := LoadQueues(service, queuePrefix)
	if err != nil {
		return err
	}

	controller := &Controller{
		queues: allQueues,
	}

	return controller.RootLoop()

}

func postMessage(body []byte, queueName string) error {
	var err error
	runURL := fmt.Sprintf("%s/worker/%s/invoke", serverURL, queueName)
	runConnection, _, err := (&websocket.Dialer{}).Dial(runURL, http.Header{})
	if err != nil {
		return err
	}
	if err := runConnection.WriteMessage(websocket.TextMessage, body); err != nil {
		if websocket.IsCloseError(err) {
			runConnection = nil
			return postMessage(body, queueName)
		}
		fmt.Println("Write Message Error, not close")
		return err
	}

	for {
		_, p, err := runConnection.ReadMessage()
		if err != nil {
			fmt.Println("Read Message Error")
			return err
		}
		os.Stdout.Write(p)
		ctl := struct {
			WS string `json:"_ws"`
		}{}
		json.Unmarshal(p, &ctl)
		if ctl.WS == "done" {
			break
		}
	}

	runConnection.Close()
	runConnection = nil

	return nil

}
