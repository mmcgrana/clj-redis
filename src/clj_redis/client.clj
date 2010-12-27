(ns clj-redis.client
  (:import java.net.URI)
  (:import redis.clients.jedis.Jedis)
  (:require [clojure.string :as str])
  (:refer-clojure :exclude [get set keys]))

(def ^:private local-url "redis://127.0.0.1:6379")

(defn init [& [{:keys [url] :as opts}]]
  (let [u (URI. (or url local-url))
        h (.getHost u)
        p (.getPort u)
        i (.getUserInfo u)]
    (let [j (Jedis. h p)]
      (when i (.auth j (last (str/split i #":"))))
      j)))

(defn ping [^Jedis j]
  (.ping j))

(defn set [^Jedis j k v]
  (.set j k v))

(defn get [^Jedis j k]
  (.get j k))

(defn keys [^Jedis j & [pattern]]
  (seq (.keys j (or pattern "*"))))
