(ns clj-redis.client
  (:import java.net.URI)
  (:import (redis.clients.jedis Jedis JedisPool JedisPoolConfig JedisPubSub Transaction))
  (:import (redis.clients.util SafeEncoder))
  (:require [clojure.string :as str])
  (:refer-clojure :exclude [get set keys type]))

(def ^{:private true} local-url
  "redis://127.0.0.1:6379")

(defn init [& [{:keys [url timeout test-on-borrow] :as opts}]]
  (let [uri (URI. (or url local-url))
        tout (or timeout 2000)
        host (.getHost uri)
        port (.getPort uri)
        uinfo (.getUserInfo uri)
        pass (and uinfo (last (str/split uinfo #":")))
        config (JedisPoolConfig.)]
    (when test-on-borrow
      (.setTestOnBorrow config test-on-borrow))
    (JedisPool. config host port tout pass)))


(defn dispatcher
  [x _]
  (class x))

(defmulti lease dispatcher)
(defmethod lease JedisPool [^JedisPool p f]
  (let [j (.getResource p)]
    (try
      (f j)
      (finally
        (.returnResource p j)))))
(defmethod lease clojure.lang.PersistentArrayMap [m f]
  (let [t (clojure.core/get m :trans)]
    (f t)))

(defn static-lease [p] (.getResource p))

(defn ping [p]
  (lease p (fn [j] (.ping j))))

(defn flush-all [p]
  (lease p (fn [j] (.flushAll j))))


;; Keys

(defn exists [p ^String k]
  (lease p (fn [j] (.exists j k))))

(defn del [p ks]
  (lease p (fn [j] (.del j ^"[Ljava.lang.String;" (into-array ks)))))

(defn keys [p & [^String pattern]]
  (lease p (fn [j] (seq (.keys j (or pattern "*"))))))

(defn rename [p ^String k ^String nk]
  (lease p (fn [j] (.rename j k nk))))

(defn renamenx [p ^String k ^String nk]
  (lease p (fn [j] (.renamenx j k nk))))

(defn expire [p ^String k ^Integer s]
  (lease p (fn [j] (.expire j k s))))

(defn expireat [p ^String k ^Long ut]
  (lease p (fn [j] (.expireAt j k ut))))

(defn ttl [p ^String k]
  (lease p (fn [j] (.ttl j k))))

(defn persist [p ^String k]
  (lease p (fn [j] (.persist j k))))

(defn move [p ^String k ^Integer db]
  (lease p (fn [j] (.move j k db))))

(defn type [p ^String k]
  (lease p (fn [j] (.type j k))))


;; Strings

(defn incr [p ^String k]
  (lease p (fn [j] (.incr j k))))

(defn incrby [p ^String k ^Long v]
  (lease p (fn [j] (.incrBy j k v))))

(defn decr [p ^String k]
  (lease p (fn [j] (.decr j k))))

(defn decrby [p ^String k ^Long v]
  (lease p (fn [j] (.decrBy j k v))))

(defn get [p ^String k]
  (lease p (fn [j] (.get j k))))

(defn set [p ^String k ^String v]
  (lease p (fn [j] (.set j k v))))

(defn mget [p & keys]
  (lease p (fn [j] (.mget j ^"[Ljava.lang.String;" (into-array keys)))))

(defn mset [p & keys]
  (lease p (fn [j] (.mset j ^"[Ljava.lang.String;" (into-array keys)))))

(defn msetnx [p & keys]
  (lease p (fn [j] (.msetnx j ^"[Ljava.lang.String;" (into-array keys)))))

(defn getset [p ^String k ^String v]
  (lease p (fn [j] (.getSet j k v))))

(defn append [p ^String k ^String v]
  (lease p (fn [j] (.append j k v))))

(defn getrange [p ^String k ^Integer start ^Integer end]
  (lease p (fn [j] (.substring j k start end))))

(defn setnx [p ^String k ^String v]
  (lease p (fn [j] (.setnx j k v))))

(defn setex [p ^String k ^Integer s ^String v]
  (lease p (fn [j] (.setex j k s v))))


; Lists

(defn lpush [p ^String k ^String v]
  (lease p (fn [j] (.lpush j k v))))

(defn rpush [p ^String k ^String v]
  (lease p (fn [j] (.rpush j k v))))

(defn lset [p ^String k ^Integer i ^String v]
  (lease p (fn [j] (.lset j k i v))))

(defn llen [p ^String k]
  (lease p (fn [j] (.llen j k))))

(defn lindex [p ^String k ^Integer i]
  (lease p (fn [j] (.lindex j k i))))

(defn lpop [p ^String k]
  (lease p (fn [j] (.lpop j k))))

(defn blpop [p ks ^Integer t]
  (lease p
   (fn [j]
     (if-let [pair (.blpop j t ^"[Ljava.lang.String;" (into-array ks))]
       (seq pair)))))

(defn rpop [p ^String k]
  (lease p (fn [j] (.rpop j k))))

(defn brpop [p ks ^Integer t]
  (lease p
    (fn [j]
      (if-let [pair (.brpop j t ^"[Ljava.lang.String;" (into-array ks))]
        (seq pair)))))

(defn lrange
  [p k ^Integer start ^Integer end]
  (lease p (fn [j] (seq (.lrange j k start end)))))


; Sets

(defn sadd [p ^String k ^String m]
  (lease p (fn [j] (.sadd j k m))))

(defn srem [p ^String k ^String m]
  (lease p (fn [j] (.srem j k m))))

(defn spop [p ^String k]
  (lease p (fn [j] (.spop j k))))

(defn scard [p ^String k]
  (lease p (fn [j] (.scard j k))))

(defn smembers [p ^String k]
  (lease p (fn [j] (seq (.smembers j k)))))

(defn sismember [p ^String k ^String m]
  (lease p (fn [j] (.sismember j k m))))

(defn srandmember [p ^String k]
  (lease p (fn [j] (.srandmember j k))))

(defn smove [p ^String k ^String d ^String m]
  (lease p (fn [j] (.smembers j k d m))))


; Sorted sets

(defn zadd [p ^String k ^Double r ^String m]
  (lease p (fn [j] (.zadd j k r m))))

(defn zcount [p ^String k ^Double min ^Double max]
  (lease p (fn [j] (.zcount j k min max))))

(defn zcard [p ^String k]
  (lease p (fn [j] (.zcard j k))))

(defn zrank [p ^String k ^String m]
  (lease p (fn [j] (.zrank j k m))))

(defn zrevrank [p ^String k ^String m]
  (lease p (fn [j] (.zrevrank j k m))))

(defn zscore [p ^String k ^String m]
  (lease p (fn [j] (.zscore j k m))))

(defn zrangebyscore
  ([p ^String k ^Double min ^Double max]
    (lease p (fn [j] (seq (.zrangeByScore j k min max)))))
  ([p ^String k ^Double min ^Double max ^Integer offset ^Integer count]
    (lease p (fn [j] (seq (.zrangeByScore j k min max offset count))))))

(defn zrangebyscore-withscore
  ([p ^String k ^Double min ^Double max]
    (lease p (fn [j] (seq (.zrangeByScoreWithScore j k min max)))))
  ([p ^String k ^Double min ^Double max ^Integer offset ^Integer count]
    (lease p (fn [j] (seq (.zrangeByScoreWithScore j k min max offset count))))))

(defn zrange [p ^String k ^Integer start ^Integer end]
  (lease p (fn [j] (seq (.zrange j k start end)))))

(defn zrevrange [p ^String k ^Integer start ^Integer end]
  (lease p (fn [j] (seq (.zrevrange j k start end)))))

(defn zincrby [p ^String k ^Double s ^String m]
  (lease p (fn [j] (.zincrby j k s m))))

(defn zrem [p ^String k ^String m]
  (lease p (fn [j] (.zrem j k m))))

(defn zremrangebyrank [p ^String k ^Integer start ^Integer end]
  (lease p (fn [j] (.zremrangeByRank j k start end))))

(defn zremrangebyscore [p ^String k ^Double start ^Double end]
  (lease p (fn [j] (.zremrangeByScore j k start end))))

(defn zinterstore [p ^String d k]
  (lease p (fn [j] (.zinterstore j d ^"[Ljava.lang.String;" (into-array k)))))

(defn zunionstore [p ^String d k]
  (lease p (fn [j] (.zunionstore j d ^"[Ljava.lang.String;" (into-array k)))))


; Hashes

(defn hget [p ^String k ^String f]
  (lease p (fn [j] (.hget j k f))))

(defn hmget [p ^String k & fs]
  (lease p (fn [j] (seq (.hmget j k ^"[Ljava.lang.String;" (into-array fs))))))

(defn hset [p ^String k ^String f ^String v]
  (lease p (fn [j] (.hset j k f v))))

(defn hmset [p ^String k h]
  (lease p (fn [j] (.hmset j k h))))

(defn hsetnx [p ^String k ^String f ^String v]
  (lease p (fn [j] (.hsetnx j k f v))))

(defn hincrby [p ^String k ^String f ^Long v]
  (lease p (fn [j] (.hincrBy j k f v))))

(defn hexists [p ^String k ^String f]
  (lease p (fn [j] (.hexists j k f))))

(defn hdel [p ^String k ^String f]
  (lease p (fn [j] (.hdel j k f))))

(defn hlen [p ^String k]
  (lease p (fn [j] (.hlen j k))))

(defn hkeys [p ^String k]
  (lease p (fn [j] (.hkeys j k))))

(defn hvals [p ^String k]
  (lease p (fn [j]  (seq (.hvals j k)))))

(defn hgetall [p ^String k]
  (lease p (fn [j] (.hgetAll j k))))


; Pub-Sub

(defn publish [p ^String c ^String m]
  (lease p (fn [j] (.publish j c m))))

(defn subscribe [p chs handler]
  (let [pub-sub (proxy [JedisPubSub] []
                  (onSubscribe [ch cnt])
                  (onUnsubscribe [ch cnt])
                  (onMessage [ch msg] (handler ch msg)))]
    (lease p (fn [j]
      (.subscribe j pub-sub ^"[Ljava.lang.String;" (into-array chs))))))


; Utils

(defn safe-ba-to-string
  "Converts byte arrays to strings, leaving nils alone"
  [ba]
  (if (nil? ba)
    nil
    (SafeEncoder/encode ba)))


; Pipeline/Transaction

(defn multi [p]
  (let [j (static-lease p)
        t (.multi j)]
    {:trans t :jedis j :pool p}))

(defn exec [m]
  (let [t           (clojure.core/get m :trans)
        j           (clojure.core/get m :jedis)
        p           (clojure.core/get m :pool)
        resps       (.exec t)
        indices     (range (.size resps))
        byte-arrays (map #(.get resps %) indices)
        strings     (map safe-ba-to-string byte-arrays)]
    (.returnResource p j)
    strings))
