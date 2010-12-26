(ns clj-redis.client
  (:import java.net.URI)
  (:import redis.clients.jedis.Jedis)
  (:refer-clojure :exclude [get set]))

(def ^:private local-url "redis://127.0.0.1:6379")

(defn init [& [{:keys [url] :as opts}]]
  (let [u (URI. (or url local-url))
        h (.getHost u)
        p (.getPort u)]
    (Jedis. h p)))

(defn ping [^Jedis r]
  (.ping r))

(defn set [^Jedis r k v]
  (.set r k v))

(defn get [^Jedis r k]
  (.get r k))
