package main

import (
	"bytes"
	"io"

	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
)

type kafkaWriter struct {
	producer sarama.SyncProducer
	writer   io.Writer
	topic    string
	key      string
	buffer   *bytes.Buffer
	messages chan sarama.ProducerMessage
}

type logMessage struct {
	TimeStamp string `json:"ts"`
	Package   string `json:"package"`
	LogLine   string `json:"logline"`
}

func (w kafkaWriter) Sender() {
	for {
		select {
		//case m := <-w.messages:
		}
	}
}

func (w kafkaWriter) send() error {
	pkg := "Core"
	r := regexp.MustCompile("^--- pkg: (.*)")
	for {
		ln, err := w.buffer.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			// TODO: handle these errors?
			break
		}

		msg := ln[:len(ln)-1]
		if p := r.FindSubmatch(msg); p != nil {
			pkg = string(p[1])
		}

		lm := logMessage{time.Now().UTC().Format("2006-01-02'T'03-04-05.000'Z'-0700"),
			pkg, string(msg)}
		jm, err := json.Marshal(lm)
		if err != nil {
			fmt.Printf("json marshalling error: %v\n", err)
			continue
		}
		message := &sarama.ProducerMessage{
			Topic: w.topic,
			Key:   sarama.StringEncoder(w.key),
			Value: sarama.StringEncoder(jm),
		}

		go func(m *sarama.ProducerMessage) {
			if _, _, err := w.producer.SendMessage(message); err != nil {
				if err != nil {
					fmt.Printf("error sending message: %s\n", err)
					// TODO: handle errors, buffer, etc
				}
				err = w.writer.(io.Closer).Close()
				if err != nil {
					// TODO: handle errors, buffer, etc
				}
			}
		}(message)

		w.writer.Write(ln)
	}

	return nil
}

func (w kafkaWriter) Flush() error {
	return w.send()
}

func (w kafkaWriter) Write(b []byte) (n int, err error) {
	// TODO: support optional in-memory buffering, memory-mapped files and file buffering

	if w.producer != nil && len(w.topic) > 0 {
		n, err = w.buffer.Write(b)
		if err != nil {
			return
		}

		err = w.send()
		return
	}

	return w.writer.Write(b)
}
