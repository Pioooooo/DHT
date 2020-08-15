package main

import (
	"chord"
	"fmt"
	"strconv"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

type Node struct {
	n *chord.Node
}

func NewNode(port int) dhtNode {
	ret := new(Node)
	ret.n = new(chord.Node)
	ret.n.Init(":" + strconv.Itoa(port))
	return ret
}

func (n *Node) Run() {
	n.n.Run()
	go n.n.Maintain()
}

func (n *Node) Create() {
	_ = n.n.Create()
}

func (n *Node) Join(addr string) bool {
	return n.n.Join(addr) == nil
}

func (n *Node) Quit() {
	_ = n.n.Quit()
}

func (n *Node) ForceQuit() {
	if !n.n.On {
		return
	}
	n.n.On = false
	err := n.n.Listen.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (n *Node) Ping(addr string) bool {
	return chord.Ping(addr)
}

func (n *Node) Put(key string, value string) bool {
	return n.n.Put(key, value) == nil
}

func (n *Node) Get(key string) (bool, string) {
	return n.n.Get(key)
}

func (n *Node) Delete(key string) bool {
	return n.n.Delete(key)
}
