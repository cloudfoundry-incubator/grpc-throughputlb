package throughputlb_test

import (
	"sync/atomic"
	"time"

	throughputlb "code.cloudfoundry.org/grpc-throughputlb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ThroughputLoadBalancer", func() {
	var (
		maxRequests int64 = 10
		maxAddrs    int64 = 5
	)

	It("conforms to the gRPC Balancer interface", func() {
		// This test will fail at compilation if the ThroughputLoadBalancer
		// does not satisfy the grpc.Balancer interface.
		var i grpc.Balancer = throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		_ = i
	})

	It("sends a notification of an available address", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		Expect(lb.Notify()).To(Receive(Equal([]grpc.Address{
			{Addr: "127.0.0.1:3000", Metadata: 0},
		})))
	})

	It("blocks if no addresses are available and blocking wait is true", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)

		done := make(chan struct{})
		go func() {
			_, _, _ = lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
			close(done)
		}()

		Consistently(done).ShouldNot(BeClosed())
	})

	It("returns an error if no addresses are available and blocking wait is false", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)

		_, _, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: false})
		Expect(err).To(MatchError(grpc.Errorf(codes.Unavailable, "there is no address available")))
	})

	It("returns an error if the only address enters a down state", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		var addrs []grpc.Address
		Expect(lb.Notify()).To(Receive(&addrs))

		down := lb.Up(addrs[0])
		down(nil)

		_, _, err = lb.Get(context.Background(), grpc.BalancerGetOptions{})
		Expect(err).To(MatchError(grpc.Errorf(codes.Unavailable, "there is no address available")))
	})

	It("returns the first available address", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		var addrs []grpc.Address
		Expect(lb.Notify()).To(Receive(&addrs))

		lb.Up(addrs[0])
		addr, _, err := lb.Get(context.Background(), grpc.BalancerGetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(addr).To(Equal(addrs[0]))
	})

	It("adds an address if the current address reaches max requests", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		var addrs []grpc.Address
		Expect(lb.Notify()).To(Receive(&addrs))

		lb.Up(addrs[0])

		for i := int64(0); i < maxRequests; i++ {
			_, _, err := lb.Get(context.Background(), grpc.BalancerGetOptions{})
			Expect(err).ToNot(HaveOccurred())
		}

		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			_, _, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}()

		Eventually(lb.Notify()).Should(Receive(&addrs))
		Expect(addrs).Should(HaveLen(2))

		lb.Up(addrs[1])
		Eventually(done).Should(BeClosed())
	})

	It("does not add addresses if the addresses are released", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		var addrs []grpc.Address
		Expect(lb.Notify()).To(Receive(&addrs))

		lb.Up(addrs[0])

		for i := int64(0); i < maxRequests*5; i++ {
			_, put, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
			Expect(err).ToNot(HaveOccurred())

			put()
		}

		Expect(lb.Notify()).ToNot(Receive())
	})

	It("does not exceed the max addresses", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		go func() {
			for i := int64(0); i < maxRequests*(maxAddrs*2); i++ {
				_, _, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
				Expect(err).ToNot(HaveOccurred())
			}
		}()

		f := func() int {
			var addrs []grpc.Address
			Eventually(lb.Notify()).Should(Receive(&addrs))

			for _, a := range addrs {
				lb.Up(a)
			}

			return len(addrs)
		}

		Eventually(f).Should(BeNumerically("==", maxAddrs))
		Consistently(f).ShouldNot(BeNumerically(">", maxAddrs))
	})

	It("is goroutine safe", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests, maxAddrs)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		getter := func() {
			defer GinkgoRecover()

			for {
				_, put, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
				Expect(err).ToNot(HaveOccurred())

				put()
			}
		}

		upper := func() {
			defer GinkgoRecover()

			for {
				addrs := <-lb.Notify()

				for i, a := range addrs {
					down := lb.Up(a)

					if i == 2 {
						go down(nil)
					}
				}
			}
		}

		for i := 0; i < 50; i++ {
			go getter()
		}

		go upper()

		time.Sleep(time.Second)
	})

	It("releases an addr when the last two addrs have no requests", func() {
		lb := throughputlb.NewThroughputLoadBalancer(
			maxRequests,
			maxAddrs,
			throughputlb.WithCleanupInterval(10*time.Millisecond),
		)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		var addrCount int64
		go func() {
			for {
				addrs := <-lb.Notify()

				for _, a := range addrs {
					down := lb.Up(a)
					_ = down
				}

				atomic.StoreInt64(&addrCount, int64(len(addrs)))
			}
		}()

		var putters []func()
		for i := 0; int64(i) < maxRequests*maxAddrs; i++ {
			_, put, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
			Expect(err).ToNot(HaveOccurred())
			putters = append(putters, put)
		}

		Eventually(func() int64 {
			return atomic.LoadInt64(&addrCount)
		}).Should(Equal(int64(5)))

		a := 0
		for _, p := range putters[((maxAddrs-int64(2))*maxRequests)-1:] {
			p()
			a++
		}

		Eventually(func() int64 {
			return atomic.LoadInt64(&addrCount)
		}).Should(Equal(int64(4)))
	})
})
