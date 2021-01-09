
# Qrono

[![build](https://github.com/c2nes/qrono/workflows/build/badge.svg)](https://github.com/c2nes/qrono/actions?query=workflow%3Abuild)

Qrono is a time-ordered queue server.

> :warning: Qrono is a hobby project. **It is not production ready.**

Values in a Qrono queue are ordered by their _deadline_ and can only be dequeued once their deadline has passed. A deadline can be specified when enqueuing or requeuing a value and defaults to the current time. Values with equal deadlines are processed in FIFO order.

Qrono provides multiple interfaces. In addition to standard HTTP and gRPC interfaces, Qrono provides a Redis-compatible [RESP](https://redis.io/topics/protocol) interface allowing `redis-cli` and many existing Redis client libraries to be used.

## Quick Start

Dev releases are available on [Docker Hub](https://hub.docker.com/r/qrono/qrono),

``` shellsession
# Start Qrono and forward the RESP (Redis protocol) port
$ docker run --rm -d -p 16379:16379 --name qrono qrono/qrono:dev

# Test that server is up and responding
$ redis-cli -p 16379 PING
PONG

# Normal enqueue. Uses "now" as the default deadline providing
# traditional FIFO queue semantics.
$ redis-cli -p 16379 ENQUEUE my-queue my-value
1) (integer) 11000001                                                                                     │
2) (integer) 1610207507966

# Dequeue next value from the queue. The entry is now in the "working"
# state and must be explicitly released or requeued by ID.
$ redis-cli -p 16379 DEQUEUE my-queue
1) (integer) 11000001
2) (integer) 1610207507966
3) (integer) 1610207507966
4) (integer) 0
5) (integer) 1
6) "my-value"

# The queue has 1 entry total and 1 dequeued (i.e. "working") item.
$ redis-cli -p 16379 STAT my-queue
1) (integer) 1
2) (integer) 1

# Release the previously dequeued value.
$ redis-cli -p 16379 RELEASE my-queue 11000001
OK

# The queue is now empty.
$ redis-cli -p 16379 STAT my-queue
1) (integer) 0
2) (integer) 0

# Enqueue a value 10 seconds in the future
$ redis-cli -p 16379 ENQUEUE my-queue my-future-value DEADLINE $(( 1000 * $(date +%s) + 10000 ))
1) (integer) 11000002
2) (integer) 1610207677000

# Value will not be immediatatley available for dequeue
$ redis-cli -p 16379 DEQUEUE my-queue
(nil)

# After 10 seconds the value will be available
$ sleep 10; redis-cli -p 16379 DEQUEUE my-queue
1) (integer) 11000002
2) (integer) 1610207677000
3) (integer) 1610207667438
4) (integer) 0
5) (integer) 1
6) "my-future-value"
```

## Queue operations

### Enqueue

```
ENQUEUE queue value [DEADLINE milliseconds-timestamp]
  (integer) id
  (integer) deadline
```

Add a `value` to the `queue` with the given deadline which defaults to now. After being enqueued the value is considered _pending_.

### Dequeue

```
DEQUEUE queue
  (integer) id
  (integer) deadline
  (integer) enqueue-time
  (integer) requeue-time
  (integer) dequeue-count
  (bulk) value
```

Dequeue the next pending value from the `queue` if available. Returns `null` if the queue is empty, or the deadline of the next value in the queue lies in the future. On success, the next pending value in the queue is moved into the _dequeued_ state.

### Requeue

```
REQUEUE queue id [DEADLINE milliseconds-timestamp]
  OK
```

Requeue a dequeued value by `id`, returning it to pending status. If not specified, deadline will default to now.

### Release

```
RELEASE queue id
  OK
```

Release a dequeued value by `id`.

### Peek

```
PEEK queue
  (integer) id
  (integer) deadline
  (integer) enqueue-time
  (integer) requeue-time
  (integer) dequeue-count
  (bulk) value
```

Returns, but does not dequeue the next pending value from the `queue`. Returns `null` if the queue is empty.

### Stat

```
STAT queue
  (integer) pending-count
  (integer) dequeued-count
```

Returns statistics for the `queue` including the number of _pending_ and _dequeued_ values. The pending count includes dequeued values.
