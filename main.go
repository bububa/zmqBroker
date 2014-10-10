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

import "fmt"
import zmq "github.com/bububa/zmq4"

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
		return
	}
	defer context.Term()
	/**
	 * Open a PULL socket. It will receive messages from Producers
	 */
	pull_socket, err := context.NewSocket(zmq.PULL)
	if err != nil {
		return
	}
	defer pull_socket.Close()
	pull_socket.Bind("tcp://*:" + PULLSOCKET)
	/**
	 * Open a PUB socket so that Consumers can subscribe to messages
	 */
	pub_socket, err := context.NewSocket(zmq.PUB)
	if err != nil {
		return
	}
	defer pub_socket.Close()
	pub_socket.Bind("tcp://*:" + PUBSOCKET)

	/**
	 * Keep receiving messages and send them to all the subscribed Consumers
	 */
	for {
		msg, err := pull_socket.Recv(0)
		if err == nil {
			fmt.Printf("Received msg %s\n", string(msg))
		} else if DEBUG {
			fmt.Println("Error" + err.Error())
		}

		_, err = pub_socket.Send(msg, 0)
		if err == nil {
			continue
		} else if DEBUG {
			fmt.Println("Error" + err.Error())
		}
	}
}
