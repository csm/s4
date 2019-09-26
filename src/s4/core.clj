(ns s4.core
  (:require [clojure.core.async :as async]
            [clojure.string :as string]
            [konserve.protocols :as kp]
            [manifold.deferred :as d]
            [s4.auth :as auth]
            [superv.async :as sv]
            [clojure.data.xml :as xml]
            [clojure.data.xml.node :as node]))

(defn- drop-leading-slashes
  [path]
  (.substring path (loop [i 0]
                     (if (and (< i (count path))
                              (= \/ (.charAt path i)))
                       (recur (inc i))
                       i))))

(defn- read-bucket-object
  [request hostname]
  (let [host-header (get-in request [:headers "host"])]
    (if (and (string/ends-with? host-header hostname)
             (not= host-header hostname))
      [(.substring (get-in request [:headers "host"]) 0 (- (count host-header) (count hostname) 1))
       (drop-leading-slashes (:uri request))]
      (vec (.split (drop-leading-slashes (:uri request)) "/" 2)))))

(defn s3-handler
  [{:keys [konserve hostname]}]
  (fn [request respond error]
    (async/go
      (try
        (let [[bucket object] (read-bucket-object request hostname)]
          (case (:request-method request)
            :get (cond (empty? bucket) (let [buckets (sv/<? sv/S (kp/-get-in konserve [:bucket-meta]))
                                             buckets-response (node/element :ListAllMyBucketsResult {:xmlns "http://s3.amazonaws.com/doc/2006-03-01"}
                                                                (node/element :Owner {}
                                                                  (node/element :ID {} "S4")
                                                                  (node/element :DisplayName {} "You Know, for Data"))
                                                                (node/element :Buckets {}
                                                                  (map (fn [{:keys [name created]}]
                                                                         (node/element :Bucket {}
                                                                           (node/element :Name {} name)
                                                                           (node/element :CreationDate created))))))
                                             body (xml/emit-str buckets-response :encoding "UTF-8")]
                                         {:status 200
                                          :headers {"content-type" "text/xml"}})
                       (empty? object) "get bucket"
                       :else "get object")
            :put (error {})
            :post .
            :delete .))
        (catch Exception x
          (error x))))))

(defn aleph-async-ring-adapter
  [async-handler]
  (fn [request]
    (let [response (d/deferred)]
      (async-handler request (partial d/success! response) (partial d/error! response))
      response)))

(defn make-handler
  [system]
  (auth/authenticating-handler (s3-handler system) system))