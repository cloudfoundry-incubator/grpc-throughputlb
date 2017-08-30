package throughputlb_test

import (
	"github.com/bradylove/throughputlb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ThroughputLoadBalancer", func() {
	var maxRequests int64 = 10

	It("conforms to the gRPC Balancer interface", func() {
		// This test will fail at compilation if the ThroughputLoadBalancer
		// does not satisfy the grpc.Balancer interface.
		var i grpc.Balancer = throughputlb.NewThroughputLoadBalancer(maxRequests)
		_ = i
	})

	It("sends a notification of an available address", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)
		err := lb.Start("127.0.0.1:3000", grpc.BalancerConfig{})
		Expect(err).ToNot(HaveOccurred())

		Expect(lb.Notify()).To(Receive(Equal([]grpc.Address{
			{Addr: "127.0.0.1:3000", Metadata: 0},
		})))
	})

	It("blocks if no addresses are available and blocking wait is true", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)

		done := make(chan struct{})
		go func() {
			lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: true})
			close(done)
		}()

		Consistently(done).ShouldNot(BeClosed())
	})

	It("returns an error if no addresses are available and blocking wait is false", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)

		_, _, err := lb.Get(context.Background(), grpc.BalancerGetOptions{BlockingWait: false})
		Expect(err).To(MatchError(grpc.Errorf(codes.Unavailable, "there is no address available")))
	})

	It("blocks if the only address enters a down state", func() {
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)
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
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)
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
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)
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
		lb := throughputlb.NewThroughputLoadBalancer(maxRequests)
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
})
