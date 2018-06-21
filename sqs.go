package main

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type sqsQueue struct {
	client *sqs.SQS
	url    string
	name   string
}

func (q *sqsQueue) Length() (uint64, error) {
	res, err := q.client.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
		QueueUrl:       aws.String(q.url),
	})
	if err != nil {
		return 0, err
	}
	lenString := aws.StringValue(res.Attributes["ApproximateNumberOfMessages"])
	return strconv.ParseUint(lenString, 10, 64)
}

func (q *sqsQueue) Next() (*inflightMessage, error) {

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(20),
	}
	resp, err := q.client.ReceiveMessage(params)
	if err != nil {
		return nil, err
	}
	if len(resp.Messages) < 1 {
		return nil, io.EOF
	}
	if len(resp.Messages) > 1 {
		return nil, fmt.Errorf("SQS returned more than 1 message")
	}

	return &inflightMessage{
		queue:   q,
		message: resp.Messages[0],
	}, nil
}

func (q *sqsQueue) Publish(body []byte) (string, error) {
	params := &sqs.SendMessageInput{
		MessageBody: aws.String(string(body)),
		QueueUrl:    aws.String(q.url),
	}
	resp, err := q.client.SendMessage(params)
	return aws.StringValue(resp.MessageId), err
}

type inflightMessage struct {
	message *sqs.Message
	queue   *sqsQueue
}

func (m inflightMessage) Ack() error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.queue.url),
		ReceiptHandle: m.message.ReceiptHandle,
	}
	_, err := m.queue.client.DeleteMessage(params)
	return err
}

func (m inflightMessage) KeepAlive(duration time.Duration) error {
	seconds := int64(duration.Seconds()) + 1
	params := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.queue.url),
		ReceiptHandle:     m.message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(seconds),
	}
	_, err := m.queue.client.ChangeMessageVisibility(params)
	return err
}

func keepMessageAlive(msg inflightMessage, callback func(msg *sqs.Message) error) error {

	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second * 50):
			}

			if err := msg.KeepAlive(time.Minute); err != nil {
				fmt.Printf("Error setting visibility timeout: %s\n", err.Error())
			}
		}
	}()

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("Panic")
			}
		}()
		err = callback(msg.message)
	}()

	close(done)
	return err
}
