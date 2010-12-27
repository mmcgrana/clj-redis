(ns clj-redis.client
  (:import java.net.URI)
  (:import (redis.clients.jedis Jedis JedisPool))
  (:import org.apache.commons.pool.impl.GenericObjectPool)
  (:require [clojure.string :as str])
  (:refer-clojure :exclude [get set keys]))

(def ^:private local-url
  "redis://127.0.0.1:6379")

(defn init [& [{:keys [url timeout] :as opts}]]
  (let [uri (URI. (or url local-url))
        tout (or timeout 2000)
        host (.getHost uri)
        port (.getPort uri)
        uinfo (.getUserInfo uri)
        pass (and uinfo (last (str/split uinfo #":")))
        config (org.apache.commons.pool.impl.GenericObjectPool$Config.)
        pool (JedisPool. config host port tout pass)]
    pool))

(defn lease [^JedisPool p f]
  (let [j (.getResource p)]
    (try
      (f j)
      (finally
        (.returnResource p j)))))

(defn ping [p]
  (lease p (fn [^Jedis j] (.ping j))))

(defn get [p ^String k]
  (lease p (fn [^Jedis j] (.get j k))))

(defn set [p ^String k ^String v]
  (lease p (fn [^Jedis j] (.set j k v))))

(defn keys [p & [^String pattern]]
  (lease p (fn [^Jedis j] (seq (.keys j (or pattern "*"))))))
