# clj-redis

Clojure Redis client library.

## Usage

    (require '[clj-redis.client :as redis])
    
    (def db (redis/init))
    
    (redis/ping db)
    => "PONG"

    (redis/set db "foo" "BAR")
    => "OK"

    (redis/get db "foo")
    => "BAR"

## Notes

The connections represented by the return value of `clj-redis.client/init` are threadsafe; they are backed by a dynamic pool of connections to the Redis server.

Implemented commands:

* set
* get
* del
* exists
* keys
* lpush
* llen
* lpop
* blpop
* rpop
* brpop
* zadd
* zcard
* zrangebyscore
* zrem
* publish
* subscribe

## Installation

Depend on `[clj-redis "0.0.9"]` in your `project.clj`.
