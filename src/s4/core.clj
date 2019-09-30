(ns s4.core
  (:require [cemerick.uri :as uri]
            [clojure.core.async :as async]
            [clojure.data.xml :as xml]
            [clojure.data.xml.node :as node]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [keywordize-keys]]
            [konserve.protocols :as kp]
            [manifold.deferred :as d]
            [s4.$$$$ :as $]
            [s4.auth :as auth]
            [superv.async :as sv])
  (:import [java.time ZonedDateTime]
           [java.time.format DateTimeFormatter]))

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

(def xml-content-type {"content-type" "application/xml"})

(defn s3-handler
  [{:keys [konserve hostname request-counter cost-tracker]}]
  (fn [request respond error]
    (async/go
      (let [request-id (format "%016x" (swap! request-counter inc))]
        (try
          (log/debug :task ::s3-handler :phase :begin :request (pr-str request))
          (let [respond (fn respond-wrapper
                          [response]
                          (log/debug :task ::s3-handler :phase :end :response (pr-str response))
                          (let [body (:body response)
                                body (if (and (= "application/xml" (get-in response [:headers "content-type"]))
                                              (sequential? body))
                                       (xml/emit-str (xml/sexp-as-element body) :encoding "UTF-8")
                                       body)]
                            (respond (assoc
                                       (assoc-in response [:headers "x-amz-request-id"] request-id)
                                       :body body))))
                [bucket object] (read-bucket-object request hostname)]
            (case (:request-method request)
              :get (cond (empty? bucket)
                         (let [buckets (sv/<? sv/S (kp/-get-in konserve [:bucket-meta]))
                               buckets-response [:ListAllMyBucketsResult {:xmlns "http://s3.amazonaws.com/doc/2006-03-01"}
                                                 [:Owner {}
                                                   [:ID {} "S4"]
                                                   [:DisplayName {} "You Know, for Data"]]
                                                 [:Buckets {}
                                                   (map (fn [[bucket-name {:keys [created]}]]
                                                          [:Bucket {}
                                                           [:Name {} bucket-name]
                                                           [:CreationDate {} (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME created)]])
                                                        buckets)]]]
                           ($/-track-put-request! cost-tracker)
                           (respond {:status 200
                                     :headers xml-content-type
                                     :body buckets-response}))

                         (empty? object)
                         (let [params (keywordize-keys (uri/query->map (:query-string request)))]
                           (log/debug :task ::s3-handler :phase :get-bucket-request :bucket bucket :params params)
                           (if-let [existing-bucket (sv/<? sv/S (kp/-get-in konserve [:bucket-meta bucket]))]
                             (cond
                               (= "2" (:list-type params))
                               (do
                                 ($/-track-put-request! cost-tracker)
                                 (respond {:status 501
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NotImplemented"]
                                                  [:Resource {} (str \/ bucket)]
                                                  [:RequestId {} request-id]]}))

                               (some? (:accelerate params))
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 200
                                           :headers xml-content-type
                                           :body [:AccelerateConfiguration {:xmlns "http://s3.amazonaws.com/doc/2006-03-01/"}
                                                  [:Status {} "Suspended"]]}))

                               (some? (:cors params))
                               (let [cors (:cors existing-bucket)]
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 200
                                           :headers xml-content-type
                                           :body [:CORSConfiguration {}
                                                  (map (fn [{:keys [header origin method max-age expose-header]}]
                                                         [:CORSRule {}
                                                          (concat
                                                            (map (fn [header] [:AllowedHeader {} header]) header)
                                                            (map (fn [origin] [:AllowedOrigin {} origin]) origin)
                                                            (map (fn [method] [:AllowedMethod {} method]) method)
                                                            (map (fn [max-age] [:MaxAgeSeconds {} max-age]) max-age)
                                                            (map (fn [header] [:ExposeHeader {} header]) expose-header))])
                                                       cors)]}))

                               (some? (:encryption params)) ; todo maybe support this?
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 404
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "ServerSideEncryptionConfigurationNotFoundError"]
                                                  [:Resource (str \/ bucket)]
                                                  [:RequestId request-id]]}))

                               (some? (:acl params)) ; todo maybe support this?
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 200
                                           :headers xml-content-type
                                           :body [:AccessControlPolicy {}
                                                  [:Owner {}
                                                   [:ID {} "S4"]
                                                   [:DisplayName {} "You Know, for Data"]]
                                                  [:AccessControlList {}
                                                   [:Grant
                                                    [:Grantee {"xmlns:xsi" "http://www.w3.org/2001/XMLSchema-instance"
                                                               "xsi:type" "CanonicalUser"}
                                                     [:ID {} "S4"]
                                                     [:DisplayName {} "You Know, for Data"]]
                                                    [:Permission {} "FULL_CONTROL"]]]]}))

                               (some? (:inventory params))
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 200
                                           :headers xml-content-type
                                           :body [:InventoryConfiguration {}]}))

                               (some? (:lifecycle params))
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 404
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NoSuchLifecycleConfiguration"]
                                                  [:Resource {} (str \/ bucket)]
                                                  [:RequestId {} request-id]]}))

                               (some? (:location params))
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 200
                                           :headers xml-content-type
                                           :body [:LocationConstraint {}]}))

                               (some? (:publicAccessBlock params))
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 404
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NoSuchPublicAccessBlockConfiguration"]
                                                  [:Resource {} (str \/ bucket)]
                                                  [:RequestId {} request-id]]}))

                               :else
                               (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 501
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NotImplemented"]
                                                  [:Resource {} (str \/ bucket)]
                                                  [:RequestId {} request-id]]})))
                             (do
                               ($/-track-get-request! cost-tracker)
                               (respond {:status 404
                                         :headers xml-content-type
                                         :body [:Error {}
                                                [:Code {} "NoSuchBucket"]
                                                [:Resource {} (str \/ bucket)]
                                                [:RequestId {} request-id]]}))))

                         :else (do
                                 ($/-track-get-request! cost-tracker)
                                 (respond {:status 501
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NotImplemented"]
                                                  [:Resource {} (:uri request)]
                                                  [:RequestId {} request-id]]})))

              :put (do
                     ($/-track-put-request! cost-tracker)
                     (cond (and (not-empty bucket) (empty? object))
                           (if-let [existing-bucket (sv/<? sv/S (kp/-get-in konserve [:bucket-meta bucket]))]
                             (respond {:status 409
                                       :headers xml-content-type
                                       :body [:Error {}
                                              [:Code {} "BucketAlreadyExists"]
                                              [:Resource {} (str \/ bucket)]
                                              [:RequestId {} request-id]]})
                             (do
                               (sv/<? sv/S (kp/-assoc-in konserve [:bucket-meta bucket] {:created (ZonedDateTime/now auth/UTC)
                                                                                         :object-count 0}))
                               (respond {:status 200
                                         :headers {"location" (str \/ bucket)}})))

                           :else (respond {:status 501
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NotImplemented"]
                                                  [:Resource {} (:uri request)]
                                                  [:RequestId {} request-id]]})))

              :post (do
                      ($/-track-put-request! cost-tracker)
                      (respond {:status 501
                                :headers xml-content-type
                                :body [:Error {}
                                       [:Code {} "NotImplemented"]
                                       [:Resource {} (:uri request)]
                                       [:RequestId {} request-id]]}))

              :delete (do
                        ($/-track-get-request! cost-tracker)
                        (cond (and (not-empty bucket) (empty? object))
                              (if-let [bucket-meta (sv/<? sv/S (kp/-get-in konserve [:bucket-meta bucket]))]
                                (if (pos? (:object-count bucket-meta 0))
                                  (respond {:status 409
                                            :headers xml-content-type
                                            :body [:Error {}
                                                   [:Code {} "BucketNotEmpty"]
                                                   [:Resource {} (str \/ bucket)]
                                                   [:RequestId {} request-id]]})
                                  (do
                                    (sv/<? sv/S (kp/-dissoc konserve [:paths bucket])) ; note, these are actual vectors, not key-paths.
                                    (sv/<? sv/S (kp/-dissoc konserve [:objects bucket]))
                                    (sv/<? sv/S (kp/-update-in konserve [:bucket-meta] #(dissoc % bucket)))
                                    (respond {:status 204})))
                                (respond {:status 404
                                          :headers xml-content-type
                                          :body [:Error {}
                                                 [:Code {} "NoSuchBucket"]
                                                 [:Resource {} (str \/ bucket)]
                                                 [:RequestId {} request-id]]}))

                              (and (not-empty bucket) (not-empty object))
                              (if-let [bucket-meta (sv/<? sv/S (kp/-get-in konserve [:bucket-meta bucket]))]
                                (respond {:status 501})
                                (respond {:status 404
                                          :headers xml-content-type
                                          :body [:Error {}
                                                 [:Code {} "NoSuchBucket"]
                                                 [:Resource {} (str \/ bucket)]
                                                 [:RequestId {} request-id]]}))

                              :else (respond {:status 501
                                              :headers xml-content-type
                                              :body [:Error {}
                                                     [:Code {} "NotImplemented"]
                                                     [:Resource {} (:uri request)]
                                                     [:RequestId {} request-id]]})))

              (do
                ($/-track-get-request! cost-tracker)
                (respond {:status 405
                          :headers xml-content-type
                          :body [:Error {}
                                 [:Code {} "MethodNotAllowed"]
                                 [:Message {} (string/upper-case (name (:request-method request)))]
                                 [:Resource {} (:uri request)]
                                 [:RequestId {} request-id]]}))))
          (catch Exception x
            (respond {:status 500
                      :headers xml-content-type
                      :body [:Error {}
                             [:Code {} "InternalError"]
                             [:Resource {} (:uri request)]
                             [:RequestId {} request-id]]})))))))

(defn aleph-async-ring-adapter
  [async-handler]
  (fn [request]
    (let [response (d/deferred)]
      (async-handler request (partial d/success! response) (partial d/error! response))
      response)))

(defn make-handler
  [system]
  (auth/authenticating-handler (s3-handler system) system))

(defn make-reloadable-handler
  "Create an S3 handler that will rebuild the handler functions on
  every request; this way you can reload the namespace in between
  calls via a REPL. System is an atom containing a system map."
  [system]
  (fn [request respond error]
    (let [handler (s3-handler @system)
          auth-handler (auth/authenticating-handler handler @system)]
      (auth-handler request respond error))))