gRPC Throughput Load Balancer [![GoDoc][go-doc-badge]][go-doc] [![travis][travis-badge]][travis] [![slack.cloudfoundry.org][slack-badge]][loggregator-slack]
===============================================================================

The gRPC throughput load balancer is a load balancer that implements the
[`grpc.Balancer
interface`](https://godoc.org/google.golang.org/grpc#Balancer). It will open a
configured number of connections to a single address and not allow more than a
given number of concurrent requests per address.

In your code when you make a gRPC request (stream or RPC), the gRPC through
load balancer will return the connection with the least number of active
requests.

## Example

``` go
lb := throughputlb.NewThroughputLoadBalancer(100, 20)

conn, err := grpc.Dial(os.Getenv("GRPC_ADDR"),
    grpc.WithBalancer(lb))
if err != nil {
    panic(err)
}
```

[go-doc-badge]:      https://godoc.org/code.cloudfoundry.org/grpc-throughputlb?status.svg
[go-doc]:            https://godoc.org/code.cloudfoundry.org/grpc-throughputlb
[slack-badge]:       https://slack.cloudfoundry.org/badge.svg
[loggregator-slack]: https://cloudfoundry.slack.com/archives/loggregator
[travis-badge]:      https://travis-ci.org/cloudfoundry-incubator/grpc-throughputlb.svg?branch=master
[travis]:            https://travis-ci.org/cloudfoundry-incubator/grpc-throughputlb?branch=master
