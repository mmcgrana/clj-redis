(defproject clj-redis "0.0.13-SNAPSHOT"
  :description "Clojure Redis client library"
  :url "https://github.com/mmcgrana/clj-redis"
  :dependencies [[org.clojure/clojure "[1.2.0,1.3.0]"]
                 [redis.clients/jedis "1.5.2"]]
  :multi-deps {"1.3" [[org.clojure/clojure "1.3.0"]
                      [redis.clients/jedis "1.5.2"]]
               "1.2" [[org.clojure/clojure "1.2.1"]
                      [redis.clients/jedis "1.5.2"]]}
  :dev-deps [[lein-multi "1.0.0"]])
