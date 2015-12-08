/*
  Copyright 2012 Xion Inc
                xion.inc@gmail.com

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package main

import (
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

/**
 * Change PULLSOCKET and PUBSOCKET if you want to start the Broker to
 * listen on other ports.
 */
const (
	PULLSOCKET         = "5000"
	PUBSOCKET          = "6000"
	HEARTBEAT_INTERVAL = 2
	DEBUG              = true
)

func main() {
	context, err := zmq.NewContext()
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer context.Term()
	/**
	 * Open a PULL socket. It will receive messages from Producers
	 */
	pullSocket, err := context.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer pullSocket.Close()
	err = pullSocket.Bind("tcp://*:" + PULLSOCKET)
	if err != nil {
		log.Fatalln(err)
		return
	}
	/**
	 * Open a PUB socket so that Consumers can subscribe to messages
	 */
	pubSocket, err := context.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer pubSocket.Close()
	err = pubSocket.Bind("tcp://*:" + PUBSOCKET)
	if err != nil {
		log.Fatalln(err)
		return
	}
	log.Println("ZMQ BROKER STARTED")
	exitChan := make(chan struct{}, 1)
	readyToQuitChan := make(chan struct{}, 1)
	// Detect Server Exit
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM)
		<-ch
		exitChan <- struct{}{}
		close(ch)
	}()

	go func(s *zmq.Socket, exitCh chan struct{}, readyToQuitCh chan struct{}) {
		pingTicker := time.NewTicker(HEARTBEAT_INTERVAL * time.Second) // 定时Flush
		for {
			select {
			case <-pingTicker.C:
				s.Send("PING", 0)
			case <-exitCh:
				pingTicker.Stop()
				readyToQuitCh <- struct{}{}
				return
			}
		}
	}(pubSocket, exitChan, readyToQuitChan)

	/**
	 * Keep receiving messages and send them to all the subscribed Consumers
	 */
	go func(pull *zmq.Socket, pub *zmq.Socket) {
		for {
			msg, err := pull.Recv(0)
			if err == nil {
				log.Printf("Received msg %s\n", msg)
			} else if DEBUG {
				log.Printf("Error:%s\n", err.Error())
				break
			}

			_, err = pub.Send(msg, 0)
			if err == nil {
				continue
			} else if DEBUG {
				log.Printf("Error:%s\n", err.Error())
				break
			}
		}
	}(pullSocket, pubSocket)
	<-readyToQuitChan
	log.Println("ZMQ BROKER QUIT")
}
