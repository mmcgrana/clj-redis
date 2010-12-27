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

## Installation

Depend on `[clj-redis "0.0.4"]` in your `project.clj`.
