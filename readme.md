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

## Installation

Depend on `[clj-redis "0.0.4"]` in your `project.clj`.
