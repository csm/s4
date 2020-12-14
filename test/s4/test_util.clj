(ns s4.test-util
  (:require [s4.core :as s4])
  (:import (java.time Instant Clock ZoneOffset)
           (java.net InetSocketAddress)))

(def ^:dynamic *s4* nil)
(def ^:dynamic *port* nil)

(def access-key "ACCESSKEY")
(def secret-key "SECRET/ACCESS/KEY")

(def epoch 0x5d000000) ; arbitrary

(def secs (atom epoch))

(defn static-clock
  ([secs] (static-clock secs ZoneOffset/UTC))
  ([secs zone]
   (proxy [Clock] []
     (getZone [] zone)
     (withZone [zone] (static-clock secs zone))
     (instant []
       (let [rules (.getRules zone)
             offset (.getOffset rules (Instant/ofEpochSecond @secs))
             offset-secs (.getTotalSeconds offset)]
         (Instant/ofEpochSecond (+ @secs offset-secs)))))))

(defn fixture
  [f]
  (let [s4 (s4/make-server! {:clock (static-clock secs)})]
    (swap! (-> @s4 :auth-store :access-keys) assoc access-key secret-key)
    (try
      (binding [*s4* s4
                *port* (-> @s4 :bind-address (.getPort))]
        (f))
      (finally
        (.close (:server @s4))))))