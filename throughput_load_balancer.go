package throughputlb

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type addrState int64

const (
	stateDown addrState = iota
	stateUp
)

type address struct {
	grpc.Address

	mu             sync.RWMutex
	state          addrState
	activeRequests int64
	maxRequests    int64
}

func (a *address) claim() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.activeRequests >= a.maxRequests {
		return errors.New("max requests exceeded")
	}

	a.activeRequests++

	return nil
}

func (a *address) release() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.activeRequests--
}

func (a *address) goUp() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.state = stateUp
}

func (a *address) goDown(_ error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// TODO: Handle error

	a.state = stateDown
}

func (a *address) isUp() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.state == stateUp
}

func (a *address) isDown() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.state == stateDown
}

func (a *address) hasCapacity() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.activeRequests < a.maxRequests
}

type ThroughputLoadBalancer struct {
	mu    sync.RWMutex
	addrs []*address

	target      string
	notify      chan []grpc.Address
	maxRequests int64
	maxAddrs    int64
}

func NewThroughputLoadBalancer(maxRequests int64, maxAddrs int64) *ThroughputLoadBalancer {
	return &ThroughputLoadBalancer{
		notify:      make(chan []grpc.Address, 1),
		addrs:       make([]*address, 0, maxAddrs),
		maxRequests: maxRequests,
		maxAddrs:    maxAddrs,
	}
}

func (lb *ThroughputLoadBalancer) Start(target string, cfg grpc.BalancerConfig) error {
	lb.target = target
	lb.addAddr()

	// TODO: Start monitor to cleanup addrs

	return nil
}

func (lb *ThroughputLoadBalancer) Up(addr grpc.Address) func(error) {
	lb.mu.RLock()
	addrs := lb.addrs
	lb.mu.RUnlock()

	var downFunc func(error)
	for _, a := range addrs {
		if a.Address == addr {
			a.goUp()

			downFunc = a.goDown
		}
	}

	return downFunc
}

func (lb *ThroughputLoadBalancer) Get(ctx context.Context, opts grpc.BalancerGetOptions) (grpc.Address, func(), error) {
	addr, err := lb.next(opts.BlockingWait)
	if err != nil {
		return grpc.Address{}, func() {}, err
	}

	return addr.Address, addr.release, nil
}

func (lb *ThroughputLoadBalancer) Notify() <-chan []grpc.Address {
	return lb.notify
}

func (*ThroughputLoadBalancer) Close() error {
	// TODO: Should this remove all addresses and notify or just stop opperation?

	return nil
}

func (lb *ThroughputLoadBalancer) sendNotify() {
	lb.mu.RLock()
	addrs := lb.addrs
	lb.mu.RUnlock()

	var grpcAddrs []grpc.Address
	for _, a := range addrs {
		grpcAddrs = append(grpcAddrs, a.Address)
	}

	lb.notify <- grpcAddrs
}

func (lb *ThroughputLoadBalancer) next(wait bool) (*address, error) {
	for {
		needNewAddr := true

		lb.mu.RLock()
		addrs := lb.addrs
		lb.mu.RUnlock()

		for _, a := range addrs {
			if a.isDown() {
				needNewAddr = false
				continue
			}

			if a.isUp() && a.hasCapacity() {
				err := a.claim()
				if err != nil {
					continue
				}

				return a, nil
			}
		}

		if needNewAddr {
			lb.addAddr()
		}

		if !wait {
			return nil, grpc.Errorf(codes.Unavailable, "there is no address available")
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (lb *ThroughputLoadBalancer) addAddr() {
	lb.mu.Lock()
	if int64(len(lb.addrs)) < lb.maxAddrs {
		lb.addrs = append(lb.addrs, &address{
			Address: grpc.Address{
				Addr:     lb.target,
				Metadata: len(lb.addrs),
			},
			maxRequests: lb.maxRequests,
		})
	}
	lb.mu.Unlock()

	lb.sendNotify()
}
