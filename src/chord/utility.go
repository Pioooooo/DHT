package chord

import (
	"crypto/sha1"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

const (
	tryTime = 3
)

var (
	two  = big.NewInt(2)
	ceil = new(big.Int).Exp(two, big.NewInt(int64(M)), nil)
)

func hashStr(s string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(s))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

func getID(n *big.Int, p int) *big.Int {
	return new(big.Int).Mod(new(big.Int).Add(new(big.Int).Exp(two, big.NewInt(int64(p)), nil), n), ceil)
}

func inRange(l, x, r *big.Int) bool {
	switch l.Cmp(r) {
	case 1:
		return !(r.Cmp(x) < 0 && x.Cmp(l) <= 0)
	case -1:
		return l.Cmp(x) < 0 && x.Cmp(r) <= 0
	default:
		return true
	}
}

func GetLocalAddress() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("Failed to find network interfaces")
	}
	for _, i := range ifaces {
		if i.Flags&net.FlagLoopback == 0 && i.Flags&net.FlagUp != 0 {
			addrs, err := i.Addrs()
			if err != nil {
				panic("Failed to get address of network interface")
			}
			for _, addr := range addrs {
				ip, ok := addr.(*net.IPNet)
				if ok {
					if ip4 := ip.IP.To4(); len(ip4) == net.IPv4len {
						return ip4.String()
					}
				}
			}
		}
	}
	panic("Failed to find valid address")
}

func Ping(addr string) bool {
	for i := 0; i < tryTime; i++ {
		c := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", addr)
			if err == nil {
				_ = client.Close()
				c <- true
			} else {
				c <- false
			}
		}()
		select {
		case ok := <-c:
			if ok {
				return true
			}
			continue
		case <-time.After(second / 2):
			break
		}
	}
	return false
}

func Dial(ip string) *rpc.Client {
	for i := 1; i <= tryTime; i++ {
		client, err := rpc.Dial("tcp", ip)
		if err != nil {
			time.Sleep(second / 2)
		} else {
			return client
		}
	}
	return nil
}
