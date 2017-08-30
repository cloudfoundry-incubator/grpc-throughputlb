package throughputlb

import (
	"errors"
	"sync"
	"sync/atomic"
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

	state          addrState
	activeRequests int64
	maxRequests    int64
}

func (a *address) claim() error {
	if atomic.AddInt64(&a.activeRequests, 1) > a.maxRequests {
		a.release()

		return errors.New("max requests exceeded")
	}

	return nil
}

func (a *address) release() {
	atomic.AddInt64(&a.activeRequests, -1)
}

type ThroughputLoadBalancer struct {
	mu    sync.RWMutex
	addrs []*address

	target      string
	notify      chan []grpc.Address
	maxRequests int64
}

func NewThroughputLoadBalancer(maxRequests int64) *ThroughputLoadBalancer {
	return &ThroughputLoadBalancer{
		notify:      make(chan []grpc.Address, 1),
		maxRequests: maxRequests,
	}
}

func (lb *ThroughputLoadBalancer) Start(target string, cfg grpc.BalancerConfig) error {
	// TODO: Resolve DNS

	lb.target = target
	lb.addAddr()

	return nil
}

func (lb *ThroughputLoadBalancer) Up(addr grpc.Address) func(error) {
	lb.mu.RLock()
	addrs := lb.addrs
	lb.mu.RUnlock()

	downFunc := func(error) {}
	for _, a := range addrs {
		if a.Address == addr {
			atomic.StoreInt64((*int64)(&a.state), (int64)(stateUp))

			downFunc = func(error) {
				atomic.StoreInt64((*int64)(&a.state), (int64)(stateDown))
			}
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
			if (addrState)(atomic.LoadInt64((*int64)(&a.state))) == stateDown {
				needNewAddr = false
				continue
			}

			if (addrState)(atomic.LoadInt64((*int64)(&a.state))) == stateUp {
				if atomic.LoadInt64(&a.activeRequests) < lb.maxRequests {
					err := a.claim()
					if err != nil {
						continue
					}

					return a, nil
				}
			}
		}

		if needNewAddr {
			lb.addAddr()
		}

		if !wait {
			return nil, grpc.Errorf(codes.Unavailable, "there is no address available")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lb *ThroughputLoadBalancer) addAddr() {
	lb.mu.Lock()
	lb.addrs = append(lb.addrs, &address{
		Address: grpc.Address{
			Addr:     lb.target,
			Metadata: len(lb.addrs),
		},
		maxRequests: lb.maxRequests,
	})
	lb.mu.Unlock()

	lb.sendNotify()
}
