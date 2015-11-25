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
)

/**
 * Change PULLSOCKET and PUBSOCKET if you want to start the Broker to
 * listen on other ports.
 */
const (
	PULLSOCKET = "5000"
	PUBSOCKET  = "6000"
	DEBUG      = true
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
	/**
	 * Keep receiving messages and send them to all the subscribed Consumers
	 */
	for {
		msg, err := pullSocket.Recv(0)
		if err == nil {
			log.Printf("Received msg %s\n", string(msg))
		} else if DEBUG {
			log.Printf("Error:%s\n", err.Error())
		}

		_, err = pubSocket.Send(msg, 0)
		if err == nil {
			continue
		} else if DEBUG {
			log.Printf("Error:%s\n", err.Error())
		}
	}
	log.Println("ZMQ BROKER QUIT")
}
