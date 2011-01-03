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

(defn exists [p ^String k]
  (lease p (fn [^Jedis j] (.exists j k))))

(defn del [p ks]
  (lease p (fn [^Jedis j] (.del j ^"[Ljava.lang.String;" (into-array ks)))))

(defn keys [p & [^String pattern]]
  (lease p (fn [^Jedis j] (seq (.keys j (or pattern "*"))))))

(defn lpush [p ^String k ^String v]
  (lease p (fn [^Jedis j] (.lpush j k v))))

(defn llen [p ^String k]
  (lease p (fn [^Jedis j] (.llen j k))))

(defn lpop [p ^String k]
  (lease p (fn [^Jedis j] (.lpop j k))))

(defn blpop [p ks ^Integer t]
  (if-let [pair (lease p (fn [^Jedis j] (.blpop j t ^"[Ljava.lang.String;" (into-array ks))))]
    (seq pair)))

(defn rpop [p ^String k]
  (lease p (fn [^Jedis j] (.rpop j k))))

(defn brpop [p ks ^Integer t]
  (if-let [pair (lease p (fn [^Jedis j] (.brpop j t ^"[Ljava.lang.String;" (into-array ks))))]
    (seq pair)))

(defn zadd [p ^String k ^Double r ^String m]
  (lease p (fn [^Jedis j] (.zadd j k r m))))

(defn zcard [p ^String k]
  (lease p (fn [^Jedis j] (.zcard j k))))

(defn zrangebyscore
  ([p ^String k ^Double min ^Double max]
    (seq (lease p (fn [^Jedis j] (.zrangeByScore j k min max)))))
  ([p ^String k ^Double min ^Double max ^Integer offset ^Integer count]
    (seq (lease p (fn [^Jedis j] (.zrangeByScore j k min max offset count))))))

(defn zrem [p ^String k ^String m]
  (lease p (fn [^Jedis j] (.zrem j k m))))
