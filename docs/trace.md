# Message trace discovery and lifecycle

`consumer.WithTrace` and `producer.WithTrace` remain available with the existing
`primitive.TraceConfig`. A trace initialization failure is logged and disables
tracing for that option; it does not prevent the business interceptor from
running or replace its result/error. No public fields were added to `TraceConfig`.

## Sharing with dynamic discovery

For a long-running process that creates and closes consumers independently, use
`consumer.WithSharedTrace` (or `producer.WithSharedTrace`) with a stable logical
cluster key and a resolver factory:

```go
shared := primitive.SharedTraceClientConfig{
    Key: "discovery-endpoint/tenant/cluster",
    ResolverFactory: func() (primitive.NsResolver, func(), error) {
        // Create an independently owned resolver here. For a dynamic resolver,
        // return its stop/close function as the second result.
        return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), nil, nil
    },
}

c, err := rocketmq.NewPushConsumer(
    consumer.WithGroupName("orders"),
    consumer.WithNameServer([]string{"127.0.0.1:9876"}),
    consumer.WithSharedTrace(&primitive.TraceConfig{}, shared),
)
if err != nil {
    return err
}
defer c.Shutdown()
// Subscribe, then Start as usual.
```

The example uses fixed addresses. To handle NameServer replacement, the factory
must return a resolver that discovers current addresses on each `Resolve` call.
The factory itself is not called again when the addresses change.

The key must identify the discovery source and destination, including any tenant
or namespace that affects routing. Do not use a consumer group, a resolved IP
list, or a changing address-list hash. `TraceConfig.UnitName`, `Access`, and the
full `Credentials` additionally partition the pool. Changing credentials creates
an isolated resource; users of the old credentials keep their resource until
closed. Different keys never share transport, even if they currently resolve to
the same addresses.

The factory is called once per shared resource lifetime. All callers using the
same key and partition must supply equivalent factory configuration. The factory
owns an independent resolver: do not return a consumer's resolver if that
consumer will close it. The optional cleanup runs once after the last dispatcher
releases the resource, or if initialization fails. Factory, `Resolve`, and cleanup
must return promptly; `Resolve` must be safe for concurrent calls. The SDK does
not call arbitrary `Close` methods on borrowed resolvers.

`TraceConfig.NamesrvAddrs` and `Resolver` are ignored by `WithSharedTrace`. The
factory is authoritative. Initialization failure is logged and tracing remains
disabled for that dispatcher; recreating the consumer/producer retries setup.

## Connections, refresh and shutdown

Each consumer/producer has its own trace dispatcher and buffer. Dispatchers in the
same partition share one NameServer connection pool, one Broker connection pool,
one resolver, and shared address and route refresh workers. Adding consumers
therefore does not add independent trace connection pools for that partition.
Connections are opened lazily; this is not a promise of exactly one TCP
connection per cluster.

NameServer addresses refresh after 10 seconds and then every 2 minutes. Trace
topic routes refresh every 30 seconds, including region-specific cloud trace
topics. NameServer discovery and Broker route discovery are separate operations;
updates are periodic, not immediate. Address discovery runs independently so a
slow route query cannot delay discovering replacement NameServers. Concurrent
cache misses reuse the first successful route lookup; scheduled refreshes still
query the servers even when a cached route exists. All dispatchers read the same
route and address state. Closing one dispatcher leaves other users active.

Route queries retain the existing 6-second timeout per NameServer and try the
next address after a failed attempt. Shutdown can cancel the whole query.
Trace records are sent in batches of 100, or after more than 5 milliseconds
without a new record, preserving the legacy batching policy.

Shutdown rejects new records, drains accepted records, and waits for in-flight
sends. Close waits at most 5 seconds, then cancels outstanding I/O; cleanup finishes
when workers and callbacks exit. Trace delivery remains best effort. After the
last dispatcher finishes, both connection pools are closed and factory cleanup
runs. A concurrent acquisition waits for that cleanup before creating a new
resource. Failed consumer/producer construction or startup also releases trace
ownership.

Legacy `WithTrace` still uses the default sharing partition and checks resolved
addresses before reuse. Its resolver is borrowed and must remain usable for the
whole shared lifetime. It also benefits from reference-counted cleanup, a common
NameServer object, route refresh, and nil-safe interceptors. Use `WithSharedTrace`
when separate resolvers can return different address snapshots for the same
logical cluster. The general producer/consumer client registry retains its
NameServer conflict checks. Each successful client acquisition holds a reference
until shutdown, even if the producer or consumer has not started. Closing the
last owner removes the registry entry so that the instance can be created again.
