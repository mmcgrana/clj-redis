(ns clj-redis.client
  (:import java.net.URI)
  (:import redis.clients.jedis.Jedis)
  (:refer-clojure :exclude [get set keys]))

(def ^:private local-url "redis://127.0.0.1:6379")

(defn init [& [{:keys [url] :as opts}]]
  (let [u (URI. (or url local-url))
        h (.getHost u)
        p (.getPort u)]
    (Jedis. h p)))

(defn ping [^Jedis j]
  (.ping j))

(defn set [^Jedis j k v]
  (.set j k v))

(defn get [^Jedis j k]
  (.get j k))

(defn keys [^Jedis j & [pattern]]
  (seq (.keys j (or pattern "*"))))
