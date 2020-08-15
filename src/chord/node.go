package chord

import (
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	M                = 160
	SucLen           = 20
	second           = time.Second
	MaintainInterval = second / 4
)

type IP struct {
	Addr string
	Id   big.Int
}

type KVPair struct {
	Key, Value string
}

type KVMap struct {
	Map  map[string]string
	Lock sync.Mutex
}

type Node struct {
	Self    IP
	Finger  [M]IP
	Pre     IP
	Suc     [SucLen]IP
	next    int
	Data    KVMap
	DataPre KVMap
	Lock    sync.Mutex

	On     bool
	Joined bool
	Server *rpc.Server
	Listen net.Listener
}

func (n *Node) Init(port string) {
	n.Self.Addr = GetLocalAddress() + port
	n.Self.Id = *hashStr(n.Self.Addr)
	n.Joined = false
	n.Data.Map = make(map[string]string)
	n.Server = new(rpc.Server)
	err := n.Server.Register(n)
	if err != nil {
		fmt.Println(err.Error())
		panic(nil)
	}
}

func (n *Node) Run() {
	listen, err := net.Listen("tcp", n.Self.Addr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	n.Listen = listen
	n.On = true
	go n.Server.Accept(listen)
	go n.Maintain()
}

func (n *Node) Stop() {
	if !n.On {
		return
	}
	n.On = false

}

func (n *Node) Create() error {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	n.Suc[0] = n.Self
	n.Pre = n.Self
	for i := 0; i < M; i++ {
		n.Finger[i] = n.Self
	}
	n.Joined = true
	return nil
}

func (n *Node) Join(ip string) error {
	n.Lock.Lock()
	client := Dial(ip)
	n.Lock.Unlock()
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: Dial Connect failure")
	}
	ret := new(IP)
	err := client.Call("Node.FindSuc", &n.Self.Id, ret)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	_ = n.UpdateSuc(ret, nil)
	for i := 0; i < M; i++ {
		tmp := new(IP)
		_ = n.FindSuc(getID(&n.Self.Id, i), tmp)
		n.Lock.Lock()
		n.Finger[i] = *tmp
		n.Lock.Unlock()
	}
	n.Lock.Lock()
	_ = client.Close()
	client = Dial(n.Suc[0].Addr)
	n.Lock.Unlock()

	tmp := make(map[string]string)
	err = client.Call("Node.ExtractData", &n.Self.Id, &tmp)
	_ = client.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	n.Data.Lock.Lock()
	n.Data.Map = tmp
	n.Data.Lock.Unlock()

	n.Joined = true
	return nil
}

func (n *Node) Quit() error {
	if !n.On {
		fmt.Println("Error: Node is off.")
		return errors.New("error: node is off")
	}
	n.On = false
	if err := n.FixSuc(); err != nil {
		return err
	}
	if n.Suc[0].Addr == n.Self.Addr {
		n.Joined = false
		n.Data.Lock.Lock()
		n.Data.Map = make(map[string]string)
		n.Data.Lock.Unlock()
		n.DataPre.Lock.Lock()
		n.DataPre.Map = make(map[string]string)
		n.DataPre.Lock.Unlock()
		return nil
	}
	n.Lock.Lock()
	client := Dial(n.Suc[0].Addr)
	n.Lock.Unlock()
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: Dial Connect failure")
	}
	n.Data.Lock.Lock()
	tmp := n.Data.Map
	n.Data.Lock.Unlock()
	err := client.Call("Node.ImportData", tmp, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	n.DataPre.Lock.Lock()
	tmp = n.DataPre.Map
	n.DataPre.Lock.Unlock()
	err = client.Call("Node.SetDataPre", tmp, nil)
	_ = client.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	if n.Pre.Addr != "" {
		client = Dial(n.Pre.Addr)
		if client == nil {
			fmt.Println("Error: Dial Connect failure.")
			return errors.New("error: Dial Connect failure")
		}
		n.Lock.Lock()
		suc := n.Suc[0]
		n.Lock.Unlock()
		err = client.Call("Node.UpdateSuc", &suc, nil)
		_ = client.Close()
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		client = Dial(n.Suc[0].Addr)
		if client == nil {
			fmt.Println("Error: Dial Connect failure.")
			return errors.New("error: Dial Connect failure")
		}
		n.Lock.Lock()
		pre := n.Pre
		n.Lock.Unlock()
		err = client.Call("Node.UpdatePre", &pre, nil)
		_ = client.Close()
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	}
	n.Joined = false
	return nil
}

func (n *Node) ClosestPrecedingNode(id *big.Int) IP {
	tmp := make(map[string]bool)
	n.Lock.Lock()
	defer n.Lock.Unlock()
	for i := M - 1; i >= 0; i-- {
		if tmp[n.Finger[i].Addr] {
			n.Finger[i] = n.Self
			continue
		}
		if inRange(&n.Self.Id, &n.Finger[i].Id, id) {
			ret := n.Finger[i]
			if Ping(ret.Addr) {
				return ret
			} else {
				tmp[ret.Addr] = true
				n.Finger[i] = n.Self
			}
		}
	}
	return IP{}
}

func (n *Node) FindSuc(id *big.Int, ret *IP) error {
	if err := n.FixSuc(); err != nil {
		return err
	}
	n.Lock.Lock()
	ok := Ping(n.Suc[0].Addr)
	n.Lock.Unlock()
	if !ok {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: dial connect failure")
	}
	sId := hashStr(n.Suc[0].Addr)

	if inRange(&n.Self.Id, id, sId) {
		*ret = n.Suc[0]
		return nil
	}
	nxt := n.ClosestPrecedingNode(id)
	if nxt.Addr == "" && inRange(&n.Self.Id, sId, id) {
		nxt = n.Suc[0]
	}
	if nxt.Addr == "" {
		fmt.Println("Error: Unable to find successor.")
		return errors.New("error: unable to find successor")
	}
	client := Dial(nxt.Addr)
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: dial connect failure")
	}
	defer func() { _ = client.Close() }()

	err := client.Call("Node.FindSuc", id, ret)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

// func (n *Node) FixFinger() error {
// 	n.Lock.Lock()
// 	defer n.Lock.Unlock()
// 	defer func() { n.next = (n.next + 1) % M }()
// 	ip := new(IP)
// 	err := n.FindSuc(getID(&n.Self.Id, n.next), ip)
// 	if err != nil {
// 		fmt.Println(err.Error())
// 		return err
// 	}
// 	n.Finger[n.next] = *ip
// 	return nil
// }

func (n *Node) FixSuc() error {
	n.Lock.Lock()
	for i := 0; i < SucLen; i++ {
		if Ping(n.Suc[i].Addr) {
			suc := n.Suc[i]
			flag := i != 0
			if flag {
				copy(n.Suc[:], n.Suc[i:])
				n.Lock.Unlock()
				time.Sleep(MaintainInterval * 6 / 5)
				client := Dial(suc.Addr)
				if client == nil {
					fmt.Println("Error: Dial Connect failure.")
					return errors.New("error: Dial Connect failure")
				}
				defer func() { _ = client.Close() }()
				err := client.Call("Node.Notify", &n.Self, nil)
				if err != nil {
					fmt.Println(err.Error())
					return err
				}
			} else {
				n.Lock.Unlock()
			}
			return nil
		}
	}
	n.Lock.Unlock()
	fmt.Println("Error: All successors failed.")
	fmt.Println(n.Self.Addr, n.Suc)
	return errors.New("error: all successors failed")
}

func (n *Node) Maintain() {
	for n.On {
		if n.Joined {
			_ = n.CheckPre()
			_ = n.Stablize()
			_ = n.MaintainSuc()
		}
		time.Sleep(MaintainInterval)
	}
}

func (n *Node) UpdateSuc(suc *IP, _ *bool) error {
	n.Lock.Lock()
	if suc.Addr == n.Suc[0].Addr {
		n.Lock.Unlock()
		return nil
	}
	copy(n.Suc[1:], n.Suc[:])
	n.Suc[0] = *suc
	n.Lock.Unlock()

	client := Dial(suc.Addr)
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: dial connect failure")
	}
	n.Data.Lock.Lock()
	tmp := n.Data.Map
	n.Data.Lock.Unlock()
	err := client.Call("Node.SetDataPre", tmp, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func (n *Node) UpdatePre(ip *IP, _ *bool) error {
	n.Lock.Lock()
	defer n.Lock.Unlock()
	n.Pre = *ip
	return nil
}

func (n *Node) CheckPre() error {
	n.Lock.Lock()
	pAddr := n.Pre.Addr
	n.Lock.Unlock()
	if pAddr != "" && !Ping(pAddr) {
		n.MergeDataPre()
		if err := n.FixSuc(); err != nil {
			return err
		}
		n.Lock.Lock()
		client := Dial(n.Suc[0].Addr)
		n.Lock.Unlock()

		if client == nil {
			fmt.Println("Error: Dial Connect failure.")
			return errors.New("error: Dial Connect failure")
		}
		defer func() { _ = client.Close() }()
		n.Data.Lock.Lock()
		tmp := n.Data.Map
		n.Data.Lock.Unlock()

		err := client.Call("Node.SetDataPre", tmp, nil)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		n.Pre.Addr = ""
	}
	return nil
}

func (n *Node) Notify(pre *IP, suc *bool) error {
	if n.Pre.Addr == "" {
		n.Lock.Lock()
		n.Pre = *pre
		n.Lock.Unlock()
		client := Dial(pre.Addr)
		if client == nil {
			fmt.Println("Error: Dial Connect failure.")
			return errors.New("error: Dial Connect failure")
		}
		defer func() { _ = client.Close() }()
		tmp := make(map[string]string)
		err := client.Call("Node.GetData", new(int), &tmp)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		_ = n.SetDataPre(tmp, nil)
	} else {
		n.Lock.Lock()
		if !Ping(n.Pre.Addr) {
			n.Lock.Unlock()
			fmt.Println("Error: Dial Connect failure.")
			return errors.New("error: Dial Connect failure")
		}
		n.Lock.Unlock()
		pId := hashStr(n.Pre.Addr)
		if inRange(pId, &pre.Id, &n.Self.Id) {
			n.Lock.Lock()
			n.Pre = *pre
			n.Lock.Unlock()
			client := Dial(pre.Addr)
			if client == nil {
				fmt.Println("Error: Dial Connect failure.")
				return errors.New("error: Dial Connect failure")
			}
			defer func() { _ = client.Close() }()
			tmp := make(map[string]string)
			err := client.Call("Node.GetData", new(int), &tmp)
			if err != nil {
				fmt.Println(err.Error())
				return err
			}
			_ = n.SetDataPre(tmp, nil)
		}
	}
	return nil
}

func (n *Node) Id(_ *int, ret *big.Int) error {
	*ret = n.Self.Id
	return nil
}

func (n *Node) Stablize() error {
	if err := n.FixSuc(); err != nil {
		return err
	}
	n.Lock.Lock()
	client := Dial(n.Suc[0].Addr)
	n.Lock.Unlock()
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: Dial Connect failure")
	}
	ret := new(IP)
	err := client.Call("Node.GetPre", new(int), ret)
	_ = client.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	if !Ping(ret.Addr) {
		return nil
	}
	sId := hashStr(n.Suc[0].Addr)
	if inRange(&n.Self.Id, &ret.Id, sId) {
		_ = n.UpdateSuc(ret, nil)
	}
	n.Lock.Lock()
	client = Dial(n.Suc[0].Addr)
	n.Lock.Unlock()
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: Dial Connect failure")
	}
	err = client.Call("Node.Notify", &n.Self, nil)
	_ = client.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func (n *Node) GetPre(_ *int, ret *IP) error {
	n.Lock.Lock()
	*ret = n.Pre
	n.Lock.Unlock()
	return nil
}

func (n *Node) MaintainSuc() error {
	n.Lock.Lock()
	pAddr := n.Pre.Addr
	n.Lock.Unlock()
	if pAddr != "" && pAddr != n.Self.Addr {
		client := Dial(pAddr)
		if client == nil {
			fmt.Println("Error: Dial Connect failure.")
			return errors.New("error: Dial Connect failure")
		}
		defer func() { _ = client.Close() }()
		n.Lock.Lock()
		var tmp [SucLen - 1]IP
		copy(tmp[:], n.Suc[1:])
		n.Lock.Unlock()
		err := client.Call("Node.SetSuc", &tmp, nil)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	}
	return nil
}

func (n *Node) SetSuc(suc *[SucLen - 1]IP, _ *bool) error {
	n.Lock.Lock()
	copy(n.Suc[1:], (*suc)[:])
	n.Lock.Unlock()
	return nil
}
