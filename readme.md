# clj-redis

Clojure Redis client library, with pipelining!

## Usage

    (require '[clj-redis.client :as redis])
    
    (def db (redis/init))
    
    (redis/ping db)
    => "PONG"

    (redis/set db "foo" "BAR")
    => "OK"

    (redis/get db "foo")
    => "BAR"
    
    (redis/lpush db "doit" "harder")
    (redis/lpush db "doit" "better")
    (redis/lpush db "doit" "faster")
    (redis/lpush db "doit" "stronger")
    
    (let [pipeline (redis/multi db)]
      (repeatedly 4 #(redis/rpop pipeline "doit"))
      (redis/exec pipeline))
    => ("harder" "better" "faster" "stronger")

## Notes

The connections represented by the return value of `clj-redis.client/init` are 
threadsafe; they are backed by a dynamic pool of connections to the Redis 
server. Pipeline objects (those returned by `clj-redis.client/multi`) are not
thread-safe, so just create new ones for each thread.


## Installation

Depend on `[org.clojars.strongh/clj-redis "0.0.19"]` in your `project.clj`.
