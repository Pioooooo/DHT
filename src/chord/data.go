package chord

import (
	"errors"
	"fmt"
	"math/big"
)

func (n *Node) PutInside(kv KVPair, _ *bool) error {
	n.Data.Lock.Lock()
	n.Data.Map[kv.Key] = kv.Value
	n.Data.Lock.Unlock()

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
	err := client.Call("Node.PutDataPre", kv, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func (n *Node) GetInside(key string, val *string) error {
	n.Data.Lock.Lock()
	defer n.Data.Lock.Unlock()
	str, ok := n.Data.Map[key]
	*val = str
	if !ok {
		fmt.Println("Error: Value not found.")
		return errors.New("error: value not found")
	}
	return nil
}

func (n *Node) DeleteInside(key string, _ *bool) error {
	n.Data.Lock.Lock()
	_, ok := n.Data.Map[key]
	if !ok {
		n.Data.Lock.Unlock()
		fmt.Println("Error: Value not found.")
		return errors.New("error: value not found")
	}
	delete(n.Data.Map, key)
	n.Data.Lock.Unlock()
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
	err := client.Call("Node.DeleteDataPre", key, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func (n *Node) ExtractData(id *big.Int, ret *map[string]string) error {
	n.Data.Lock.Lock()
	defer n.Data.Lock.Unlock()
	for k, v := range n.Data.Map {
		if !inRange(id, hashStr(k), &n.Self.Id) {
			(*ret)[k] = v
			delete(n.Data.Map, k)
		}
	}
	return nil
}

func (n *Node) GetData(_ *int, ret *map[string]string) error {
	n.Data.Lock.Lock()
	*ret = n.Data.Map
	n.Data.Lock.Unlock()
	return nil
}

func (n *Node) ImportData(dat map[string]string, _ *bool) error {
	n.Data.Lock.Lock()
	defer n.Data.Lock.Unlock()
	for k, v := range dat {
		n.Data.Map[k] = v
	}
	return nil
}

func (n *Node) Put(key, val string) error {
	tmp := new(IP)
	err := n.FindSuc(hashStr(key), tmp)
	if err != nil {
		return err
	}
	client := Dial(tmp.Addr)
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return errors.New("error: Dial Connect failure")
	}
	defer func() { _ = client.Close() }()
	err = client.Call("Node.PutInside", KVPair{key, val}, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func (n *Node) Get(key string) (bool, string) {
	tmp := new(IP)
	ret := new(string)
	err := n.FindSuc(hashStr(key), tmp)
	if err != nil {
		fmt.Println(err.Error())
		return false, *ret
	}
	client := Dial(tmp.Addr)
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return false, *ret
	}
	defer func() { _ = client.Close() }()
	err = client.Call("Node.GetInside", key, ret)
	if err != nil {
		fmt.Println(err.Error())
		return false, *ret
	}
	return true, *ret
}

func (n *Node) Delete(key string) bool {
	tmp := new(IP)
	err := n.FindSuc(hashStr(key), tmp)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	client := Dial(tmp.Addr)
	if client == nil {
		fmt.Println("Error: Dial Connect failure.")
		return false
	}
	defer func() { _ = client.Close() }()
	err = client.Call("Node.DeleteInside", key, nil)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	return true
}

func (n *Node) MergeDataPre() {
	n.DataPre.Lock.Lock()
	n.Data.Lock.Lock()
	for k, v := range n.DataPre.Map {
		n.Data.Map[k] = v
	}
	n.Data.Lock.Unlock()
	n.DataPre.Map = make(map[string]string)
	n.DataPre.Lock.Unlock()
}

func (n *Node) SetDataPre(dat map[string]string, _ *bool) error {
	n.DataPre.Lock.Lock()
	n.DataPre.Map = dat
	n.DataPre.Lock.Unlock()
	return nil
}

func (n *Node) PutDataPre(kv KVPair, _ *bool) error {
	n.DataPre.Lock.Lock()
	n.DataPre.Map[kv.Key] = kv.Value
	n.DataPre.Lock.Unlock()
	return nil
}

func (n *Node) DeleteDataPre(key string, _ *bool) error {
	n.DataPre.Lock.Lock()
	_, ok := n.DataPre.Map[key]
	if ok {
		delete(n.DataPre.Map, key)
	}
	n.DataPre.Lock.Unlock()
	return nil
}
