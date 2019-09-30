(ns s4.$$$$
  (:require [clojure.core.async :as async]
            [konserve.protocols :as kp])
  (:import [java.util Date]))

(defprotocol ICostTracker
  (-track-put-request! [_]
    "Track a PUT, COPY, POST or LIST request")
  (-track-get-request! [_]
    "Track a GET or all other requests.")
  (-track-data-in! [_ bytes]
    "Track bytes data transfer in.")
  (-track-data-out! [_ bytes]
    "Track bytes data transfer out.")
  (-get-storage-used [_]
    "Get the amount of data stored.")
  (get-cost-estimate [_ until-date]
    "Get a cost estimate. If until-date is not specified, gives you the
    instantaneous cost based only on request count and data transfer.
    With a until-date (a java.util.Date), compute the request and transfer
    pricing, plus the storage costs from now until that date."))

(defn compute-tiered-cost
  [amount tiers]
  (if (zero? amount)
    0
    (let [[tier & tiers] tiers
          [tier-amount tier-cost] tier
          used-this-tier (min amount tier-amount)
          cost-this-tier (* used-this-tier tier-cost)]
      (+ cost-this-tier (compute-tiered-cost (- amount used-this-tier) tiers)))))

(defrecord CostTracker [puts gets data-in data-out konserve cost-config]
  ICostTracker
  (-track-put-request! [_] (swap! puts inc))
  (-track-get-request! [_] (swap! gets inc))
  (-track-data-in! [_ bytes] (swap! data-in + bytes))
  (-track-data-out! [_ bytes] (swap! data-out + bytes))
  (-get-storage-used [_]
    (if-let [buckets (async/<!! (kp/-get-in konserve [:bucket-meta]))]
      (reduce + 0 (remove nil? (map :total-bytes (vals buckets))))
      0))
  (get-cost-estimate [this until-date]
    (let [base-costs {:put-requests (* @puts (get cost-config :put-request))
                      :get-requests (* @gets (get cost-config :get-request))
                      :data-in (compute-tiered-cost @data-in (get cost-config :data-in))
                      :data-out (compute-tiered-cost @data-out (get cost-config :data-out))}
          costs (if (some? until-date)
                  (let [duration (- (.getTime ^Date until-date) (System/currentTimeMillis))]
                    (assoc base-costs
                      :storage (compute-tiered-cost (* (-get-storage-used this) duration)
                                                    (get cost-config :storage))))
                  base-costs)]
      (assoc costs :total (reduce + (vals costs))))))

(extend-protocol ICostTracker
  nil
  (-track-put-request! [_])
  (-track-get-request! [_])
  (track-data-in! [_ _])
  (-track-data-out! [_ _])
  (-get-storage-used [_])
  (get-cost-estimate [_ _]))

(def GB (* 1024 1024 1024))
(def TB (* GB 1024))
(def month (* 30 24 60 60 1000))

; example config for us-west-2 (Oregon), standard storage class,
; all transfer out to the Internet
(with-precision 100
  (def us-west-2-standard
    {:put-request (/ 0.0005M 1000)
     :get-request (/ 0.00004M 1000)
     :data-in [[Long/MAX_VALUE 0M]]
     :data-out [[GB 0M]
                [(* 9.999M TB) (/ 0.09M GB)]
                [(* 40 TB) (/ 0.085M GB)]
                [(* 100 TB) (/ 0.07M GB)]
                [Long/MAX_VALUE (/ 0.05M GB)]]
     :storage [[(* 50 TB) (/ 0.023M GB month)]
               [(* 450 TB) (/ 0.022M GB month)]
               [Long/MAX_VALUE (/ 0.021M GB month)]]}))

(defn cost-tracker
  ([konserve] (cost-tracker konserve us-west-2-standard))
  ([konserve cost-config] (->CostTracker (atom 0) (atom 0) (atom 0) (atom 0) konserve cost-config)))