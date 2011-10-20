(ns clj-redis.test.client
  (:refer-clojure :exclude [get set keys type])
  (:use [clojure.test]
        [clj-redis.client]))

(deftest test-client
  (let [db (init :url "redis://localhost")
        [k v] ["foo" "bar"]]
    (is (= "PONG" (ping db)))
    (set db k v)
    (is (= v (get db k)))))