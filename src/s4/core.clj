(ns s4.core
  (:require [cemerick.uri :as uri]
            [clojure.core.async :as async]
            [clojure.data.xml :as xml]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [keywordize-keys]]
            [konserve.core :as k]
            [konserve.protocols :as kp]
            [konserve.serializers :as ser]
            [s4.$$$$ :as $]
            [s4.auth :as auth]
            [s4.sqs.queues :as queues]
            [superv.async :as sv]
            [s4.util :as util :refer [xml-content-type s3-xmlns]]
            [clojure.spec.alpha :as s]
            [clojure.data.json :as json]
            [clojure.core.async.impl.protocols :as impl])
  (:import [java.time ZonedDateTime ZoneId Instant]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [java.nio ByteBuffer]
           [java.security SecureRandom MessageDigest]
           [java.util Base64 List UUID]
           [com.google.common.io ByteStreams]
           [java.io ByteArrayInputStream InputStream]
           [org.fressian.handlers ReadHandler WriteHandler]
           [clojure.lang PersistentTreeMap]))

(def write-handlers
  "Write handlers for fressian, for durable konserve stores."
  (atom {PersistentTreeMap
         {"sorted-map"
          (reify WriteHandler
            (write [_ wtr m]
              (.writeTag wtr "sorted-map" 1)
              (.writeObject wtr (mapcat identity m))))}
         ZonedDateTime
         {"zoned-inst"
          (reify WriteHandler
            (write [_ wtr t]
              (.writeTag wtr "zoned-inst" 2)
              (.writeInt wtr (-> ^ZonedDateTime t ^Instant (.toInstant) (.toEpochMilli)))
              (.writeString wtr (.getId (.getZone ^ZonedDateTime t)))))}}))

(def read-handlers
  "Read handlers for fressian, for durable konserve stores."
  (atom {"sorted-map"
         (reify ReadHandler
           (read [_ rdr tag component-count]
             (let [kvs ^List (.readObject rdr)]
               (PersistentTreeMap/create (seq kvs)))))
         "zoned-inst"
         (reify ReadHandler
           (read [_ rdr tag component-count]
             (let [milli (.readInt rdr)
                   zone-id ^String (.readObject rdr)]
               (ZonedDateTime/ofInstant (Instant/ofEpochMilli milli)
                                        (ZoneId/of zone-id)))))}))

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
      [(uri/uri-decode (.substring (get-in request [:headers "host"]) 0 (- (count host-header) (count hostname) 1)))
       (uri/uri-decode (drop-leading-slashes (:uri request)))]
      (vec (map uri/uri-decode (.split (drop-leading-slashes (:uri request)) "/" 2))))))

(s/def ::S3EventType #{"s3:ReducedRedundancyLostObject"
                       "s3:ObjectCreated:*"
                       "s3:ObjectCreated:Put"
                       "s3:ObjectCreated:Post"
                       "s3:ObjectCreated:Copy"
                       "s3:ObjectCreated:CompleteMultipartUpload"
                       "s3:ObjectRemoved:*"
                       "s3:ObjectRemoved:Delete"
                       "s3:ObjectRemoved:DeleteMarkerCreated"
                       "s3:ObjectRestore:Post"
                       "s3:ObjectRestore:Completed"})

(s/def :QueueConfiguration/Event (s/coll-of ::S3EventType))

(s/def ::FilterRuleName (fn [[k v]]
                          (and (= :Name k)
                               (#{"prefix" "suffix"} v))))
(s/def ::FilterRuleValue (fn [[k v]]
                           (and (= :Value k)
                                (string? v))))
(s/def ::FilterRule (fn [rule]
                      (when-let [[k & args] rule]
                        (and (= :FilterRule k)
                             (= 2 (count args))
                             (some #(s/valid? ::FilterRuleName %) args)
                             (some #(s/valid? ::FilterRuleValue %) args)))))
(s/def ::Filter (fn [f]
                  (when-let [[k & rules] f]
                    (and (= :S3Key k)
                         (every? #(s/valid? ::FilterRule %) rules)))))

(s/def :QueueConfiguration/Id string?)
(s/def :QueueConfiguration/Queue string?)

(s/def ::QueueConfiguration (s/coll-of (s/keys :req-un [:QueueConfiguration/Event
                                                        :QueueConfiguration/Queue]
                                               :opt-un [::Filter
                                                        :QueueConfiguration/Id])))

(s/def :TopicConfiguration/Event (s/coll-of ::S3EventType))
(s/def :TopicConfiguration/Id string?)
(s/def :TopicConfiguration/Topic string?)

(s/def ::TopicConfiguration (s/coll-of (s/keys :req-un [:TopicConfiguration/Event
                                                        :TopicConfiguration/Topic]
                                               :opt-un [::Filter
                                                        :TopicConfiguration/Id])))

(s/def :LambdaFunctionConfiguration/Event (s/coll-of ::S3EventType))
(s/def :LambdaFunctionConfiguration/Id string?)
(s/def :LambdaFunctionConfiguration/CloudFunction string?)

(s/def ::LambdaFunctionConfiguration (s/coll-of (s/keys :req-un [:LambdaFunctionConfiguration/Event
                                                                 :LambdaFunctionConfiguration/CloudFunction]
                                                        :opt-un [::Filter
                                                                 :LambdaFunctionConfiguration/Id])))

(s/def ::NotificationConfiguration (s/keys :opt-un [::QueueConfiguration
                                                    ::TopicConfiguration
                                                    ::LambdaFunctionConfiguration]))

(defn ^:no-doc element->map
  ([element] (element->map element
                           (fn [content]
                             (if-let [children (not-empty (filter xml/element? content))]
                               (map element->map children)
                               content))))
  ([element value-fn]
   (when (some? element)
     {(:tag element) (value-fn (:content element))})))

(defn ^:no-doc assoc-some
  [m k v & kvs]
  (let [m (if (some? v) (assoc m k v) m)]
    (if-let [[k v & kvs] kvs]
      (apply assoc-some m k v kvs)
      m)))

(defn ^:no-doc element-as-sexp
  [element]
  (vec (concat [(:tag element)]
               (some-> (:attrs element) not-empty vector)
               (if (some xml/element? (:content element))
                 (map element-as-sexp (filter xml/element? (:content element)))
                 (:content element)))))

(defn ^:no-doc parse-notification-configuration
  [xml]
  (log/debug "parse-notification-configuration" (pr-str xml))
  (if (= :NotificationConfiguration (:tag xml))
    (let [config (->> (:content xml)
                      (filter xml/element?)
                      (reduce (fn [config element]
                                (if-let [key (#{:QueueConfiguration :TopicConfiguration :CloudFunctionConfiguration} (:tag element))]
                                  (merge-with concat config {key [(as-> {} conf
                                                                        (assoc-some conf
                                                                                    :Event
                                                                                    (->> (:content element)
                                                                                         (filter #(and (xml/element? %) (= :Event (:tag %))))
                                                                                         (mapcat :content)
                                                                                         not-empty))
                                                                        (assoc-some conf
                                                                                    :Filter
                                                                                    (some->> (:content element)
                                                                                             (filter #(and (xml/element? %) (= :Filter (:tag %))))
                                                                                             (first)
                                                                                             (:content)
                                                                                             (filter xml/element?)
                                                                                             (first)
                                                                                             (element-as-sexp)))
                                                                        (->> (:content element)
                                                                             (filter #(and (xml/element? %) (#{:Id :Queue :Topic :CloudFunction} (:tag %))))
                                                                             (mapcat #(vector (:tag %) (->> (:content %) (first))))
                                                                             (apply assoc conf)))]})
                                  (throw (IllegalArgumentException. "invalid notification configuration"))))
                              {}))
          _ (log/debug "config before conform:" (pr-str config))
          config (s/conform ::NotificationConfiguration config)]
      (if (= ::s/invalid config)
        (throw (IllegalArgumentException. "invalid notification configuration"))
        config))
    (throw (IllegalArgumentException. "invalid notification configuration"))))

(defn ^:no-doc bucket-put-ops
  [bucket request {:keys [konserve clock sqs-server]} request-id]
  (sv/go-try sv/S
    (let [params (keywordize-keys (uri/query->map (:query-string request)))]
      (if (empty? params)
        (if-let [existing-bucket (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))]
          {:status 409
           :headers xml-content-type
           :body [:Error
                  [:Code "BucketAlreadyExists"]
                  [:Resource (str \/ bucket)]
                  [:RequestId request-id]]}
          (do
            (sv/<? sv/S (k/assoc-in konserve [:bucket-meta bucket] {:created (ZonedDateTime/now clock)
                                                                    :object-count 0}))
            (sv/<? sv/S (k/assoc-in konserve [:version-meta bucket] (sorted-map)))
            {:status 200
             :headers {"location" (str \/ bucket)}}))
        (if-let [existing-bucket (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket]))]
          (cond (some? (:versioning params))
                (if-let [config (some-> (:body request) (xml/parse :namespace-aware false))]
                  (if (= "VersioningConfiguration" (name (:tag config)))
                    (if-let [new-state (#{"Enabled" "Suspended"} (some->> config
                                                                          :content
                                                                          (filter #(= :Status (:tag %)))
                                                                          (first)
                                                                          :content
                                                                          (first)))]
                      (let [current-state (:versioning existing-bucket)]
                        (when (not= current-state new-state)
                          (sv/<? sv/S (k/assoc-in konserve [:bucket-meta bucket :versioning] new-state)))
                        {:status 200})
                      {:status 400
                       :headers xml-content-type
                       :body [:Error
                              [:Code "IllegalVersioningConfigurationException"]
                              [:Resource (str \/ bucket)]
                              [:RequestId request-id]]})
                    {:status 400
                     :headers xml-content-type
                     :body [:Error
                            [:Code "IllegalVersioningConfigurationException"]
                            [:Resource (str \/ bucket)]
                            [:RequestId request-id]]})
                  {:status 400
                   :headers xml-content-type
                   :body [:Error
                          [:Code "MissingRequestBodyError"]
                          [:Resource (str \/ bucket)]
                          [:RequestId request-id]]})

                (some? (:tagging params))
                (if-let [config (some-> (:body request) (xml/parse :namespace-aware false))]
                  (if (= :Tagging (:tag config))
                    (let [tags (->> (:content config)
                                    (filter #(= :TagSet (:tag %)))
                                    first
                                    :content
                                    (filter #(= :Tag (:tag %)))
                                    (map :content)
                                    (map (fn [tags] (filter #(#{:Key :Value} (:tag %)) tags)))
                                    (map (fn [tags] [(->> tags (filter #(= :Key (:tag %))) first :content first)
                                                     (->> tags (filter #(= :Value (:tag %))) first :content first)])))]
                      (sv/<? sv/S (k/assoc-in konserve [:bucket-meta bucket :tags] tags))
                      {:status 204}))
                  {:status 400
                   :headers xml-content-type
                   :body [:Error
                          [:Code "MissingRequestBodyError"]
                          [:Resource (str \/ bucket)]
                          [:RequestId request-id]]})

                (some? (:notification params))
                (try
                  (if-let [config (some-> (:body request) (xml/parse :namespace-aware false) (parse-notification-configuration))]
                    (cond (or (not-empty (:TopicConfiguration config))
                              (not-empty (:CloudFunctionConfiguration config)))
                          (throw (IllegalArgumentException.))

                          (not-empty (:QueueConfiguration config))
                          (if-let [queue-metas (when (some? sqs-server)
                                                 (->> (:QueueConfiguration config)
                                                      (map #(k/get-in (-> sqs-server deref :konserve) [:queue-meta (:QueueArn %)]))
                                                      (sv/<?* sv/S)))]
                            (do
                              (sv/<? sv/S (k/assoc-in konserve [:bucket-meta bucket :notifications] (:QueueConfiguration config)))
                              {:status 200})
                            (throw (IllegalArgumentException.))))

                    {:status 400
                     :headers xml-content-type
                     :body [:Error
                            [:Code "MissingRequestBodyError"]
                            [:Resource (str \/ bucket)]
                            [:RequestId request-id]]})
                  (catch IllegalArgumentException _
                    {:status 400
                     :headers xml-content-type
                     :body [:Error [:Code "InvalidArgument"] [:Resource (str \/ bucket)] [:RequestId request-id]]}))

                ; todo cors - too lazy to parse out the damn xml right now

                :else
                {:status 200
                 :headers xml-content-type
                 :body [:Error [:Code "NotImplemented"] [:Resource (str \/ bucket)] [:RequestId request-id]]})
          {:status 404
           :headers xml-content-type
           :body [:Error [:Code "NoSuchBucket"] [:Resource (str \/ bucket)] [:RequestId request-id]]})))))

(defn ^:no-doc list-objects-v2
  [bucket bucket-meta request {:keys [konserve]} request-id]
  (sv/go-try sv/S
    (let [{:keys [delimiter encoding-type max-keys prefix continuation-token fetch-owner start-after]
           :or {max-keys "1000"}}
          (keywordize-keys (uri/query->map (:query-string request)))
          max-keys (Integer/parseInt max-keys)
          encode-key (fn [k] (if (= "url" encoding-type) (uri/uri-encode k) k))
          objects (remove nil? (map (fn [[k v]] (when-not (:deleted? (first v))
                                                  (assoc (first v) :key k)))
                                    (sv/<? sv/S (k/get-in konserve [:version-meta bucket]))))
          objects (drop-while (fn [{:keys [key]}]
                                (neg? (compare key (or continuation-token start-after))))
                              objects)
          objects (if (and (some? delimiter) (some? prefix))
                    (filter #(string/starts-with? (:key %) prefix) objects)
                    objects)
          common-prefixes (when (some? delimiter)
                            (->> objects
                                 (map :key)
                                 (dedupe)
                                 (filter #(or (nil? prefix) (string/starts-with? % prefix)))
                                 (filter #(string/includes? (subs % (some-> (or prefix "") count)) delimiter))
                                 (map #(str prefix (subs % (some-> (or prefix "") count) (string/index-of % delimiter (some-> (or prefix "") count)))))))
          truncated? (> (count objects) max-keys)
          objects (take max-keys objects)
          objects (if (some? delimiter)
                    (if (some? prefix)
                      (filter #(not (string/includes? (subs (:key %) (count prefix)) delimiter)) objects)
                      (filter #(not (string/includes? (:key %) delimiter)) objects))
                    objects)
          response [:ListBucketResult {:xmlns s3-xmlns}
                    [:Name bucket]
                    [:Prefix prefix]
                    [:KeyCount (count objects)]
                    [:IsTruncated truncated?]
                    [:Delimiter delimiter]
                    [:MaxKeys max-keys]]
          response (if truncated?
                     (conj response [:NextContinuationToken (:key (last objects))])
                     response)
          response (reduce conj response
                           (map (fn [object]
                                  (let [obj [:Contents
                                             [:Key (encode-key (:key object))]
                                             [:LastModified (.format (:created object) DateTimeFormatter/ISO_OFFSET_DATE_TIME)]
                                             [:ETag (:etag object)]
                                             [:Size (:content-length object)]
                                             [:StorageClass "STANDARD"]]]
                                    (if (= "true" fetch-owner)
                                      (conj obj [:Owner [:ID "S4"] [:DisplayName "You Know, for Data"]])
                                      obj)))
                                objects))
          response (reduce conj response
                           (map (fn [prefix]
                                  [:CommonPrefixes [:Prefix prefix]])
                                common-prefixes))]
      {:status 200
       :headers xml-content-type
       :body response})))

(defn ^:no-doc list-versions
  [bucket bucket-meta request {:keys [konserve]} request-id]
  (sv/go-try sv/S
    (let [{:keys [delimiter encoding-type key-marker max-keys prefix version-id-marker]
           :or {max-keys "1000" key-marker "" version-id-marker ""}}
          (keywordize-keys (uri/query->map (:query-string request)))
          encode-key (fn [k] (if (= "uri" encoding-type)
                               (uri/uri-encode k)
                               k))
          max-keys (Integer/parseInt max-keys)
          versions (sv/<? sv/S (k/get-in konserve [:version-meta bucket]))
          versions (mapcat (fn [[key versions]]
                             (let [[current & others] versions]
                               (cons (assoc current :key key :current? true)
                                     (map #(assoc % :key key) others))))
                           versions)
          versions (drop-while (fn [{:keys [key]}]
                                 (neg? (compare key key-marker)))
                               versions)
          versions (if (empty? version-id-marker)
                     versions
                     (let [seen? (atom false)]
                       (drop-while (fn [{:keys [version-id]}]
                                     (if @seen?
                                       false
                                       (do
                                         (when (= (or version-id "null") version-id-marker)
                                           (reset! seen? true))
                                         true)))
                                   versions)))
          versions (if (and (some? delimiter) (some? prefix))
                     (filter #(string/starts-with? (:key %) prefix) versions)
                     versions)
          common-prefixes (when (some? delimiter)
                            (->> versions
                                 (map :key)
                                 (dedupe)
                                 (filter #(or (nil? prefix) (string/starts-with? % prefix)))
                                 (filter #(string/includes? (subs % (some-> (or prefix "") count)) delimiter))
                                 (map #(str prefix (subs % (some-> (or prefix "") count) (string/index-of % delimiter (some-> (or prefix "") count)))))))
          versions (if (some? delimiter)
                     (if (some? prefix)
                       (filter #(not (string/includes? (subs (:key %) (count prefix)) delimiter)) versions)
                       (filter #(not (string/includes? (:key %) delimiter)) versions))
                     versions)
          truncated? (> (count versions) max-keys)
          versions (take max-keys versions)
          last-version (when truncated? (last versions))
          {:keys [delete-markers versions]} (group-by (fn [{:keys [deleted?]}]
                                                        (if deleted? :delete-markers :versions))
                                                      versions)
          result [:ListVersionsResult {:xmlns s3-xmlns}
                  [:Name bucket]
                  [:Prefix prefix]
                  [:KeyMarker key-marker]
                  [:VersionIdMarker version-id-marker]
                  [:MaxKeys max-keys]
                  [:IsTruncated truncated?]]
          result (if truncated?
                   (conj result [:NextKeyMarker (:key last-version)]
                         [:NextVersionIdMarker (or (:version-id last-version) "null")])
                   result)
          result (reduce conj result (map (fn [marker]
                                            [:DeleteMarker
                                             [:Key (encode-key (:key marker))]
                                             [:VersionId (or (:version-id marker) "null")]
                                             [:IsLatest (boolean (:current? marker))]
                                             [:LastModified (.format (:created marker) DateTimeFormatter/ISO_OFFSET_DATE_TIME)]
                                             [:Owner [:ID "S4"]]])
                                          delete-markers))
          result (reduce conj result (map (fn [version]
                                            [:Version
                                             [:Key (encode-key (:key version))]
                                             [:VersionId (or (:version-id version) "null")]
                                             [:IsLatest (boolean (:current? version))]
                                             [:LastModified (.format (:created version) DateTimeFormatter/ISO_OFFSET_DATE_TIME)]
                                             [:ETag (:etag version)]
                                             [:Size (:content-length version)]
                                             [:Owner [:ID "S4"]]
                                             [:StorageClass "STANDARD"]])
                                          versions))
          result (reduce conj result (map (fn [prefix]
                                            [:CommonPrefixes [:Prefix prefix]])
                                          common-prefixes))]
      {:status 200
       :headers xml-content-type
       :body result})))

(defn ^:no-doc list-objects
  [bucket bucket-meta request system request-id]
  (sv/go-try sv/S
    (let [request (update request :query-string
                          (fn [query-string]
                            (let [query (uri/query->map query-string)]
                              (-> query
                                  (assoc "continuation-token" (get query "marker"))
                                  (dissoc "marker")
                                  (assoc "fetch-owner" "true")
                                  (uri/map->query)))))
          response (:body (sv/<? sv/S (list-objects-v2 bucket bucket-meta request system request-id)))
          response (vec (map (fn [element]
                               (cond (keyword? element) element
                                     (map? element) element
                                     :else (condp = (first element)
                                             :ContinuationToken (into [:Marker] (rest element))
                                             :NextContinuationToken (into [:NextMarker] (rest element))
                                             element)))
                             response))]
      {:status 200
       :headers xml-content-type
       :body response})))

(defn bucket-get-ops
  [bucket request {:keys [konserve cost-tracker] :as system} request-id]
  (sv/go-try sv/S
    (let [params (keywordize-keys (uri/query->map (:query-string request)))]
      (log/debug :task ::s3-handler :phase :get-bucket-request :bucket bucket :params params)
      (if-let [existing-bucket (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))]
        (cond
          (= "2" (:list-type params))
          (do
            ($/-track-put-request! cost-tracker)
            (sv/<? sv/S (list-objects-v2 bucket existing-bucket request system request-id)))

          (some? (:versions params))
          (do
            ($/-track-put-request! cost-tracker)
            (sv/<? sv/S (list-versions bucket existing-bucket request system request-id)))

          (some? (:accelerate params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 200
             :headers xml-content-type
             :body [:AccelerateConfiguration {:xmlns s3-xmlns}
                    [:Status {} "Suspended"]]})

          (some? (:cors params))
          (let [cors (:cors existing-bucket)]
            ($/-track-get-request! cost-tracker)
            {:status 200
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
                         cors)]})

          (some? (:encryption params)) ; todo maybe support this?
          (do
            ($/-track-get-request! cost-tracker)
            {:status 404
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "ServerSideEncryptionConfigurationNotFoundError"]
                    [:Resource (str \/ bucket)]
                    [:RequestId request-id]]})

          (some? (:acl params)) ; todo maybe support this?
          (do
            ($/-track-get-request! cost-tracker)
            {:status 200
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
                      [:Permission {} "FULL_CONTROL"]]]]})

          (some? (:inventory params))
          (if (some? (:id params))
            (do
              ($/-track-get-request! cost-tracker)
              {:status 200
               :headers xml-content-type
               :body [:InventoryConfiguration {}]})
            (do
              ($/-track-put-request! cost-tracker)
              {:status 200
               :headers xml-content-type
               :body [:ListInventoryConfigurationsResult {:xmlns s3-xmlns}
                      [:IsTruncated {} "false"]]}))

          (some? (:lifecycle params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 404
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "NoSuchLifecycleConfiguration"]
                    [:Resource {} (str \/ bucket)]
                    [:RequestId {} request-id]]})

          (some? (:location params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 200
             :headers xml-content-type
             :body [:LocationConstraint {}]})

          (some? (:publicAccessBlock params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 404
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "NoSuchPublicAccessBlockConfiguration"]
                    [:Resource {} (str \/ bucket)]
                    [:RequestId {} request-id]]})

          (some? (:logging params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 200
             :headers xml-content-type
             :body [:BucketLoggingStatus {:xmlns "http://doc.s3.amazonaws.com/2006-03-01"}]})

          (some? (:metrics params))
          (if (some? (:id params))
            (do
              ($/-track-get-request! cost-tracker)
              {:status 200
               :headers xml-content-type
               :body [:MetricsConfiguration {:xmlns s3-xmlns}]})
            (do
              ($/-track-put-request! cost-tracker)
              {:status 200
               :headers xml-content-type
               :body [:ListMetricsConfigurationsResult {:xmlns s3-xmlns}
                      [:IsTruncated {} "false"]]}))

          (some? (:notification params))
          (do
            ($/-track-get-request! cost-tracker)
            (let [configs (:notifications existing-bucket)]
              (log/debug "returning configs:" (pr-str configs))
              {:status 200
               :headers xml-content-type
               :body (into [:NotificationConfiguration {:xmlns s3-xmlns}]
                           (map (fn [conf]
                                  (vec (concat [:QueueConfiguration]
                                               (map (fn [event] [:Event event]) (:Event conf))
                                               (some->> (:Filter conf) (vector :Filter) vector)
                                               (some->> (:Id conf) (vector :Id) vector)
                                               (some->> (:Queue conf) (vector :Queue) vector))))
                                configs))}))

          (or (some? (:policyStatus params)) (some? (:policy params)))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 404
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "NoSuchBucketPolicy"]
                    [:Resource (str \/ bucket)]
                    [:RequestId request-id]]})

          (some? (:versions params))
          (do
            ($/-track-put-request! cost-tracker)
            {:status 501
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "NotImplemented"]
                    [:Resource {} (str \/ bucket)]
                    [:RequestId {} request-id]]})

          (some? (:replication params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 404
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "ReplicationConfigurationNotFoundError"]
                    [:Resource {} (str \/ bucket)]
                    [:RequestId {} request-id]]})

          (some? (:requestPayment params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 200
             :headers xml-content-type
             :body [:RequestPaymentConfiguration {:xmlns s3-xmlns}
                    [:Payer {} "BucketOwner"]]})

          (some? (:tagging params))
          (do
            ($/-track-get-request! cost-tracker)
            (if-let [tags (not-empty (get existing-bucket :tags))]
              {:status 200
               :headers xml-content-type
               :body [:Tagging {}
                      [:TagSet {}
                       (map (fn [[key value]]
                              [:Tag {}
                               [:Key {} (str key)]
                               [:Value {} (str value)]])
                            tags)]]}
              {:status 404
               :headers xml-content-type
               :body [:Error {}
                      [:Code {} "NoSuchTagSet"]
                      [:Resource {} (str \/ bucket)]
                      [:RequestId {} request-id]]}))

          (some? (:versioning params))
          (do
            ($/-track-get-request! cost-tracker)
            (let [config (get existing-bucket :versioning)]
              (cond (nil? config)
                    {:status 200
                     :headers xml-content-type
                     :body [:VersioningConfiguration {:xmlns s3-xmlns}]}

                    :else
                    {:status 200
                     :headers xml-content-type
                     :body [:VersioningConfiguration {:xmlns s3-xmlns}
                            [:Status {} config]]})))

          (some? (:website params))
          (do
            ($/-track-get-request! cost-tracker)
            {:status 404
             :headers xml-content-type
             :body [:Error {}
                    [:Code {} "NoSuchWebsiteConfiguration"]
                    [:Resource {} (str \/ bucket)]
                    [:RequestId {} request-id]]})

          (some? (:analytics params))
          (do
            ($/-track-put-request! cost-tracker)
            {:status 200
             :headers xml-content-type
             :body [:ListBucketAnalyticsConfigurationResult {:xmlns s3-xmlns}
                    [:IsTruncated {} "false"]]})

          (some? (:uploads params))
          (do
            ($/-track-put-request! cost-tracker)
            (let [{:keys [delimiter encoding-type max-uploads key-marker prefix upload-id-marker]
                   :or {max-uploads "1000" key-marker "" upload-id-marker ""}}
                  params
                  max-uploads (Integer/parseInt max-uploads)
                  uploads (sv/<? sv/S (k/get-in konserve [:uploads bucket]))
                  uploads (doall (drop-while (fn [[[key upload-id]]]
                                               (and (not (pos? (compare key key-marker)))
                                                    (not (pos? (compare upload-id upload-id-marker)))))
                                             uploads))
                  uploads (doall (if (and (some? delimiter) (some? prefix))
                                   (filter #(string/starts-with? (first (key %)) prefix) uploads)
                                   uploads))
                  common-prefixes (doall (when (some? delimiter)
                                           (->> uploads
                                                (map (comp first key))
                                                (filter #(or (nil? prefix) (string/starts-with? % prefix)))
                                                (filter #(string/includes? (subs % (some-> (or prefix "") count)) delimiter))
                                                (map #(str prefix (subs % (some-> (or prefix "") count) (string/index-of % delimiter (some-> (or prefix "") count)))))
                                                (set))))
                  uploads (doall (if (some? delimiter)
                                   (if (some? prefix)
                                     (filter #(not (string/includes? (subs (first (key %)) (count prefix)) delimiter)) uploads)
                                     (filter #(not (string/includes? (first (key %)) delimiter)) uploads))
                                   uploads))
                  truncated? (> (count uploads) max-uploads)
                  uploads (take max-uploads uploads)
                  result [:ListMultipartUploadsResult {:xmlns s3-xmlns}
                          [:Bucket {} bucket]
                          [:KeyMarker {} key-marker]
                          [:UploadIdMarker {} upload-id-marker]
                          [:NextKeyMarker {} (if truncated? (first (key (last uploads))) "")]
                          [:NextUploadIdMarker {} (if truncated? (second (key (last uploads))) "")]
                          [:MaxUploads {} (str max-uploads)]
                          [:IsTruncated {} truncated?]]
                  result (if encoding-type
                           (conj result [:EncodingType {} encoding-type])
                           result)
                  result (reduce conj result
                                 (map (fn [[[key upload-id] {:keys [created]}]]
                                        [:Upload {}
                                         [:Key {} key]
                                         [:UploadId {} upload-id]
                                         [:Initiator {}
                                          [:ID {} "S4"]
                                          [:DisplayName {} "You Know, for Data"]]
                                         [:Owner {}
                                          [:ID {} "S4"]
                                          [:DisplayName {} "You Know, for Data"]]
                                         [:StorageClass {} "STANDARD"]
                                         [:Initiated {} (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME created)]])
                                      uploads))
                  result (reduce conj result
                                 (map (fn [prefix]
                                        [:CommonPrefixes {}
                                         [:Prefix {} prefix]])
                                      common-prefixes))]
              {:status 200
               :headers xml-content-type
               :body result}))

          :else
          (do
            ($/-track-put-request! cost-tracker)
            (sv/<? sv/S (list-objects bucket existing-bucket request system request-id))))

        (do
          ($/-track-get-request! cost-tracker)
          {:status 404
           :headers xml-content-type
           :body [:Error {}
                  [:Code {} "NoSuchBucket"]
                  [:Resource {} (str \/ bucket)]
                  [:RequestId {} request-id]]})))))

(def ^:no-doc random (SecureRandom.))

(defn ^:no-doc generate-blob-id
  [bucket object]
  (let [uuid (UUID/nameUUIDFromBytes (.getBytes (pr-str [bucket object])))
        b (byte-array 32)
        buf (ByteBuffer/wrap b)]
    (.putLong buf (.getMostSignificantBits uuid))
    (.putLong buf (.getLeastSignificantBits uuid))
    (.putLong buf (System/currentTimeMillis))
    (.putLong buf (.nextLong random))
    (.encodeToString (Base64/getEncoder) b)))

(defn ^:no-doc trigger-bucket-notification
  [sqs-server notification-config notification]
  (sv/go-try
    sv/S
    (when (some? sqs-server)
      (doseq [config notification-config]
        (when (or (and (= "ObjectCreated:Put" (get notification :eventName))
                       (some #{"s3:ObjectCreated:Put" "s3:ObjectCreated:*"} (:Event config)))
                  (and (= "ObjectRemoved:Delete" (get notification :eventName))
                       (some #{"s3:ObjectRemoved:Delete" "s3:ObjectRemoved:*"} (:Event config)))
                  (and (= "ObjectRemoved:DeleteMarkerCreated" (get notification :eventName))
                       (some #{"s3:ObjectRemoved:DeleteMarkerCreated" "s3:ObjectRemoved:*"} (:Event config)))
                  (every? (fn [rule]
                            (let [name (second (first (filter #(= :Name (first %)) (rest rule))))
                                  value (second (first (filter #(= :Value (first %)) (rest rule))))]
                              (if (= "prefix" name)
                                (string/starts-with? (get-in notification [:s3 :key]) value)
                                (string/ends-with? (get-in notification [:s3 :key]) value))))
                          (rest (:Filter config))))
          (when-let [queue-meta (sv/<? sv/S (k/get-in (-> sqs-server deref :konserve) [:queue-meta (:Queue config)]))]
            (let [queues (queues/get-queue (-> sqs-server deref :queues) (:Queue config))
                  message-id (str (UUID/randomUUID))
                  body (json/write-str {:Records [(if-let [id (:Id config)]
                                                    (assoc-in notification [:s3 :configurationId] id)
                                                    notification)]})
                  body-md5 (util/hex (util/md5 body))]
              (queues/offer! queues (queues/->Message message-id message-id body body-md5 {})
                             (Integer/parseInt (get-in queue-meta [:attributes :DelaySeconds]))
                             (Integer/parseInt (get-in queue-meta [:attributes :MessageRetentionPeriod]))))))))))

(defn ^:no-doc put-object
  [bucket object request {:keys [konserve cost-tracker clock sqs-server]} request-id]
  (sv/go-try sv/S
    (if-let [existing-bucket (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))]
      (let [blob-id (generate-blob-id bucket object)
            version-id (when (= "Enabled" (:versioning existing-bucket))
                         blob-id)
            content (some-> (:body request) (ByteStreams/toByteArray))
            etag (let [md (MessageDigest/getInstance "MD5")]
                   (some->> content (.update md))
                   (->> (.digest md)
                        (map #(format "%02x" %))
                        (string/join)))
            content-type (get-in request [:headers "content-type"] "binary/octet-stream")
            created (ZonedDateTime/now clock)]
        (when (some? content)
          ($/-track-data-in! cost-tracker (count content))
          (if (satisfies? kp/PBinaryAsyncKeyValueStore konserve)
            (sv/<? sv/S (k/bassoc konserve blob-id content))
            (sv/<? sv/S (k/assoc-in konserve blob-id content))))
        (sv/<? sv/S (k/update-in konserve [:version-meta bucket object]
                      (fn [versions]
                        (cons
                          {:blob-id        blob-id
                           :version-id     version-id
                           :created        (ZonedDateTime/now clock)
                           :etag           (str \" etag \")
                           :content-type   content-type
                           :content-length (count content)}
                          (if (= "Enabled" (:versioning existing-bucket))
                            versions
                            (vec (filter #(= version-id (:version-id %)) versions)))))))
        (sv/<? sv/S (trigger-bucket-notification sqs-server (:notifications existing-bucket)
                                                 {:eventVersion "2.1"
                                                  :eventSource "aws:s3"
                                                  :awsRegion "s4"
                                                  :eventTime (.format created DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                  :eventName "ObjectCreated:Put"
                                                  :userIdentity {:principalId "S4"}
                                                  :requestParameters {:sourceIPAddress (:remote-addr request)}
                                                  :responseElements {:x-amz-request-id request-id}
                                                  :s3 {:s3SchemaVersion "1.0"
                                                       :bucket {:name bucket
                                                                :ownerIdentity {:principalId "S4"}
                                                                :arn bucket}
                                                       :object {:key object
                                                                :size (count content)
                                                                :eTag etag
                                                                :versionId (or version-id "null")
                                                                :sequencer "00"}}}))
        {:status 200
         :headers (as-> {"ETag" etag} h
                        (if (some? version-id)
                          (assoc h "x-amz-version-id" version-id)
                          h))})
      {:status 404
       :headers xml-content-type
       :body [:Error
              [:Code "NoSuchBucket"]
              [:Resource (str \/ bucket)]
              [:RequestId request-id]]})))

(defn ^:no-doc parse-date-header
  [d]
  (try
    (ZonedDateTime/parse d DateTimeFormatter/RFC_1123_DATE_TIME)
    (catch DateTimeParseException _
      (try
        (ZonedDateTime/parse d auth/RFC-1036-FORMATTER)
        (catch DateTimeParseException _
          (ZonedDateTime/parse d auth/ASCTIME-FORMATTER))))))

(defn ^:no-doc parse-range
  [r]
  (let [[begin end] (rest (re-matches #"bytes=([0-9]+)-([0-9]*)" r))]
    (when (some? begin)
      [(Long/parseLong begin)
       (some-> (not-empty end) (Long/parseLong))])))

(defn ^:no-doc unwrap-input-stream
  [value]
  (log/debug :task ::unwrap-input-stream :value value)
  (cond (instance? InputStream value) value
        (map? value) (recur (:input-stream value))
        (bytes? value) (ByteArrayInputStream. value)))

(defn ^:no-doc get-object
  [bucket object request {:keys [konserve cost-tracker]} request-id with-body?]
  (sv/go-try sv/S
    ($/-track-get-request! cost-tracker)
    (if-let [existing-bucket (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))]
      (if-let [versions (sv/<? sv/S (k/get-in konserve [:version-meta bucket object]))]
        (let [params (keywordize-keys (uri/query->map (:query-string request)))]
          (cond (some? (:acl params))
                (as-> {:status 200
                       :headers xml-content-type} r
                      (if with-body?
                        (assoc r :body [:AccessControlPolicy
                                        [:Owner
                                         [:ID "S4"]
                                         [:DisplayName "You Know, for Data"]]
                                        [:AccessControlList
                                         [:Grant
                                          [:Grantee {"xmlns:xsi" "http://www.w3.org/2001/XMLSchema-instance"
                                                     "xsi:type" "CanonicalUser"}
                                           [:ID "S4"]
                                           [:DisplayName "You Know, for Data"]]
                                          [:Permission "FULL_CONTROL"]]]])
                        r))

                (or (some? (:legal-hold params))
                    (some? (:retention params))
                    (some? (:torrent params))
                    (some? (:tagging params)))
                (as-> {:status 501
                       :headers xml-content-type} r
                      (if with-body?
                        (assoc r :body [:Error
                                        [:Code "NotImplemented"]
                                        [:Resource (str \/ bucket \/ object)]
                                        [:RequestId request-id]])
                        r))

                :else
                (if-let [version (let [v (if-let [version-id (:versionId params)]
                                           (first (filter #(= version-id (:version-id %)) versions))
                                           (first versions))]
                                   (log/debug :task ::get-object :phase :got-version :version v)
                                   v)]
                  (if (:deleted? version)
                    (as-> {:status 404
                           :headers (as-> xml-content-type h
                                          (if (some? (:versioning existing-bucket))
                                            (assoc h "x-amz-version-id" (or (:version-id version) "null")
                                                     "x-amz-delete-marker" "true")
                                            h))} r
                          (if with-body?
                            (assoc r :body [:Error
                                            [:Code "NoSuchKey"]
                                            [:Resource (str \/ bucket \/ object)]
                                            [:RequestId request-id]])
                            r))
                    (let [if-modified-since (some-> (get-in request [:headers "if-modified-since"]) parse-date-header)
                          if-unmodified-since (some-> (get-in request [:headers "if-unmodified-since"]) parse-date-header)
                          if-match (get-in request [:headers "if-match"])
                          if-none-match (get-in request [:headers "if-none-match"])
                          content-type (or (:response-content-type params)
                                           (:content-type version))
                          headers (as-> {"content-type" content-type
                                         "last-modified" (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME (:created version))
                                         "accept-ranges" "bytes"
                                         "content-length" (str (:content-length version))} h
                                        (if-let [etag (:etag version)]
                                          (assoc h "etag" etag)
                                          h)
                                        (if-let [v (:response-content-language params)]
                                          (assoc h "content-language" v)
                                          h)
                                        (if-let [v (:response-expires params)]
                                          (assoc h "expires" v)
                                          h)
                                        (if-let [v (:response-cache-control params)]
                                          (assoc h "cache-control" v)
                                          h)
                                        (if-let [v (:response-content-disposition params)]
                                          (assoc h "content-disposition" v)
                                          h)
                                        (if-let [v (:response-content-encoding params)]
                                          (assoc h "content-encoding" v)
                                          h)
                                        (if (= "Enabled" (:versioning existing-bucket))
                                          (assoc h "x-amz-version-id" (or (:version-id version) "null"))
                                          h))
                          range (some-> (get-in request [:headers "range"]) parse-range)]
                      (log/debug :task ::get-object :phase :checking-object :version version
                                 :if-modified-since if-modified-since
                                 :if-unmodified-since if-unmodified-since
                                 :if-match if-match
                                 :if-none-match if-none-match)
                      (cond (and (some? if-modified-since)
                                 (pos? (compare if-modified-since (:created version))))
                            {:status  304
                             :headers headers}

                            (and (some? if-unmodified-since)
                                 (pos? (compare (:created version) if-unmodified-since)))
                            {:status  412
                             :headers headers}

                            (and (some? if-match)
                                 (not= if-match (:etag version)))
                            {:status  412
                             :headers headers}

                            (and (some? if-none-match)
                                 (= if-none-match (:etag version)))
                            {:status  304
                             :headers headers}

                            (and (some? range)
                                 (or (and (some? (second range)) (> (first range) (second range)))
                                     (> (first range) (:content-length version))
                                     (and (some? (second range)) (> (second range) (:content-length version)))))
                            {:status  416
                             :headers headers}

                            (and (some? range)
                                 (or (pos? (first range))
                                     (and (some? (second range))
                                          (not= (second range) (:content-length version)))))
                            (let [range [(first range) (or (second range) (:content-length version))]
                                  response {:status  206
                                            :headers (assoc headers
                                                       "content-range" (str "bytes " (first range) \- (second range) \/ (:content-length version)))}]
                              (if with-body?
                                (if (satisfies? kp/PBinaryAsyncKeyValueStore konserve)
                                  ; there seems to be no consistency in what -bget returns
                                  ; memory store returns the callback's value
                                  ; filestore expects a channel from the callback, takes from
                                  ; that channel, and returns that value on a new channel
                                  ; leveldb returns what the callback returns, but *within*
                                  ; a go block (so it's a channel in a channel).
                                  (let [content (k/bget konserve (:blob-id version)
                                                                 (fn [in]
                                                                   (async/thread
                                                                     (some-> (unwrap-input-stream in)
                                                                             (ByteStreams/limit (second range))
                                                                             (ByteStreams/skipFully (first range))
                                                                             (ByteStreams/toByteArray)))))
                                        content (loop [content content]
                                                  (if (satisfies? impl/ReadPort content)
                                                    (recur (sv/<? sv/S content))
                                                    content))]
                                    ($/-track-data-out! cost-tracker (- (second range) (first range)))
                                    (assoc response :body content))
                                  (let [content (or (sv/<? sv/S (k/get-in konserve (:blob-id version))) (byte-array 0))]
                                    ($/-track-data-out! cost-tracker (- (second range) (first range)))
                                    (assoc response :body (ByteArrayInputStream. content (first range) (- (second range) (first range))))))
                                response))

                            :else
                            (let [response {:status 200
                                            :headers headers}]
                              (if with-body?
                                (if (satisfies? kp/PBinaryAsyncKeyValueStore konserve)
                                  (let [content (k/bget konserve (:blob-id version)
                                                                 (fn [in]
                                                                   (async/thread
                                                                     (some-> (unwrap-input-stream in)
                                                                             (ByteStreams/toByteArray)))))
                                        content (loop [content content]
                                                  (if (satisfies? impl/ReadPort content)
                                                    (recur (sv/<? sv/S content))
                                                    content))]
                                    ($/-track-data-out! cost-tracker (:content-length version))
                                    (assoc response :body content))
                                  (let [content (or (sv/<? sv/S (k/get-in konserve (:blob-id version))) (byte-array 0))]
                                    ($/-track-data-out! cost-tracker (:content-length version))
                                    (assoc response :body (ByteArrayInputStream. content))))
                                response)))))

                  (as-> {:status 404
                         :headers xml-content-type} r
                        (if with-body?
                          (assoc r :body [:Error
                                          [:Code "NoSuchVersion"]
                                          [:Resource (str \/ bucket \/ object)]
                                          [:RequestId request-id]])
                          r)))))
        (as-> {:status 404
               :headers xml-content-type} r
              (if with-body?
                (assoc r :body [:Error
                                [:Code "NoSuchKey"]
                                [:Resource (str \/ bucket \/ object)]
                                [:RequestId request-id]])
                r)))
      (as-> {:status 404
             :headers xml-content-type} r
            (if with-body?
              (assoc r :body [:Error
                              [:Code "NoSuchBucket"]
                              [:Resource (str \/ bucket \/ object)]
                              [:RequestId request-id]])
              r)))))

(defn ^:no-doc delete-object
  [{:keys [konserve clock sqs-server]} request bucket object bucket-meta request-id]
  (sv/go-try sv/S
    ;(log/debug :task ::delete-object :phase :begin :bucket bucket :object object)
    (let [{:keys [versionId tagging]} (keywordize-keys (uri/query->map (:query-string request)))]
      (if (some? tagging)
        {:status 501
         :headers xml-content-type
         :body [:Error [:Code "NotImplemented"] [:Resource (str \/ bucket \/ object)]]}
        (let [last-mod (ZonedDateTime/now clock)
              [old new] (sv/<?
                          sv/S
                          (k/update-in konserve [:version-meta bucket]
                            (fn [objects]
                              (let [res (update objects object
                                          (fn [versions]
                                            ;(log/debug :task ::delete-object :phase :updating-version :versions versions)
                                            (if (nil? versions)
                                              versions
                                              (cond (some? versionId)
                                                    (remove #(= versionId (:version-id %)) versions)

                                                    (= "Enabled" (:versioning bucket-meta))
                                                    (let [version-id (generate-blob-id bucket object)]
                                                      (cons {:version-id    version-id
                                                             :deleted?      true
                                                             :last-modified last-mod}
                                                            versions))

                                                    (:deleted? (first versions))
                                                    versions

                                                    (and (<= (count versions) 1)
                                                         (not= "Enabled" (:versioning bucket-meta)))
                                                    nil

                                                    :else
                                                    (cons {:version-id    nil
                                                           :deleted?      true
                                                           :last-modified last-mod}
                                                          (remove #(nil? (:version-id %)) versions))))))]
                                (if (nil? (get res object))
                                  (dissoc res object)
                                  res)))))]
          ;(log/debug :task ::delete-object :phase :updated-versions :result [old new])
          (if (and (nil? old) (nil? new))
            {:status 204}
            (do
              (when-let [deleted-version (first (set/difference (set old) (set new)))]
                (when-let [blob-id (:blob-id deleted-version)]
                  (sv/<? sv/S (k/dissoc konserve blob-id))))
              (sv/<? sv/S (trigger-bucket-notification sqs-server (:notifications bucket-meta)
                                                       {:eventVersion "2.1"
                                                        :eventSource "aws:s3"
                                                        :awsRegion "s4"
                                                        :eventTime (.format last-mod DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                        :eventName (if (some? versionId)
                                                                     "ObjectRemoved:DeleteMarkerCreated"
                                                                     "ObjectRemoved:Delete")
                                                        :userIdentity {:principalId "S4"}
                                                        :requestParameters {:sourceIPAddress (:remote-addr request)}
                                                        :responseElements {:x-amz-request-id request-id}
                                                        :s3 {:s3SchemaVersion "1.0"
                                                             :bucket {:name bucket
                                                                      :ownerIdentity {:principalId "S4"}
                                                                      :arn bucket}
                                                             :object (assoc-some {:key object
                                                                                  :sequencer "00"}
                                                                                 :versionId versionId)}}))
              (cond (some? versionId)
                    {:status 204
                     :headers (as-> {"x-amz-version-id" versionId}
                                    h
                                    (if (some #(= versionId (:version-id %)) old)
                                      (assoc h "x-amz-delete-marker" "true")
                                      h))}

                    (not-empty (get new object))
                    {:status 204
                     :headers {"x-amz-version-id" (or (:version-id (first new)) "null")
                               "x-amz-delete-marker" "true"}}

                    :else
                    {:status 204
                     :headers (if (or (empty? old) (nil? (:versioning bucket-meta)))
                                {}
                                {"x-amz-version-id" "null"})}))))))))

(defn s3-handler
  "Create an asynchronous Ring handler for the S3 API.

  Keys in the argument map:

  * `konserve` The konserve instance.
  * `hostname` Your hostname, e.g. \"localhost\". This should match what your client sends.
  * `request-id-prefix` A string to prepend to request IDs.
  * `request-counter` An atom containing an int, used to generate request IDs.
  * `cost-tracker` A [[s4.$$$$/ICostTracker]], for estimating costs.
  * `clock` A `java.time.Clock` to use for generating timestamps."
  [{:keys [konserve hostname request-id-prefix request-counter cost-tracker clock sqs-server] :as system}]
  (fn [request respond error]
    (async/go
      (let [request-id (str request-id-prefix (format "%016x" (swap! request-counter inc)))]
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
            (log/debug :task ::s3-handler :bucket bucket :object object)
            (case (:request-method request)
              :head (cond (and (not-empty bucket)
                               (empty? object))
                          (do
                            ($/-track-get-request! cost-tracker)
                            (if (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))
                              (respond {:status 200})
                              (respond {:status 404})))

                          (and (not-empty bucket)
                               (not-empty object))
                          (do
                            ($/-track-get-request! cost-tracker)
                            (respond (sv/<? sv/S (get-object bucket object request system request-id false))))

                          :else
                          (do
                            ($/-track-get-request! cost-tracker)
                            (respond {:status 200})))

              :get (cond (empty? bucket)
                         (let [buckets (sv/<? sv/S (k/get-in konserve [:bucket-meta]))
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
                         (respond (sv/<? sv/S (bucket-get-ops bucket request system request-id)))

                         :else
                         (respond (sv/<? sv/S (get-object bucket object request system request-id true))))

              :put (do
                     ($/-track-put-request! cost-tracker)
                     (cond (and (not-empty bucket) (empty? object))
                           (respond (sv/<? sv/S (bucket-put-ops bucket request system request-id)))

                           (and (not-empty bucket) (not-empty object))
                           (respond (sv/<? sv/S (put-object bucket object request system request-id)))

                           :else (respond {:status 501
                                           :headers xml-content-type
                                           :body [:Error {}
                                                  [:Code {} "NotImplemented"]
                                                  [:Resource {} (:uri request)]
                                                  [:RequestId {} request-id]]})))

              :post (let [params (keywordize-keys (uri/query->map (:query-string request)))]
                      ($/-track-put-request! cost-tracker)
                      (cond
                        (and (some? (:delete params))
                             (nil? object))
                        (if-let [bucket-meta (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket]))]
                          (let [objects (element-as-sexp (xml/parse (:body request) :namespace-aware false))]
                            (if (not= :Delete (first objects))
                              (respond {:status 400
                                        :headers xml-content-type
                                        :body [:Error
                                               [:Code "MalformedXML"]
                                               [:Resource (:uri request)]
                                               [:RequestId request-id]]})
                              (let [quiet (= "true" (->> (rest objects)
                                                         (filter #(= :Quiet (first %)))
                                                         (first)
                                                         (second)))
                                    results (->> (rest objects)
                                                 (filter #(= :Object (first %)))
                                                 (map #(let [object (into {} (rest %))
                                                             query {}
                                                             query (if-let [v (:VersionId object)]
                                                                     (assoc query :versionId v))
                                                             request {:query-string (uri/map->query query)}]
                                                         (async/go
                                                           [object (async/<! (delete-object system request bucket (:Key object) bucket-meta request-id))])))
                                                 (sv/<?* sv/S)
                                                 (doall))]
                                (respond {:status 200
                                          :headers xml-content-type
                                          :body (into [:DeleteObjectOutput]
                                                      (->> results
                                                           (remove #(and quiet (= 200 (:status (second %)))))
                                                           (map (fn [[object result]]
                                                                  (if (= 200 (:status result))
                                                                    (vec (concat [:Deleted
                                                                                  [:Key (:Key object)]]
                                                                                 (some->> (:VersionId object) (vector :VersionId) (vector))
                                                                                 (when-let [v (get-in result [:headers "x-amz-version-id"])]
                                                                                   [[:DeleteMarkerVersionId v]])
                                                                                 (when (= "true" (get-in result [:headers "x-amz-delete-marker"]))
                                                                                   [[:DeleteMarker "true"]])))
                                                                    (vec (concat [:Error
                                                                                  [:Key (:Key object)]]
                                                                                 (some->> (:VersionId object) (vector :VersionId) (vector))
                                                                                 (->> (:body result)
                                                                                      (rest)
                                                                                      (filter #(= :Code (first %)))))))))))}))))
                          (respond {:status 404
                                    :headers xml-content-type
                                    :body [:Error [:Code "NoSuchBucket"] [:Resource (:uri request)] [:RequestId request-id]]}))

                        :else
                        (respond {:status 501
                                  :headers xml-content-type
                                  :body [:Error {}
                                         [:Code {} "NotImplemented"]
                                         [:Resource {} (:uri request)]
                                         [:RequestId {} request-id]]})))

              :delete (do
                        ($/-track-get-request! cost-tracker)
                        (cond (and (not-empty bucket) (empty? object))
                              (if-let [bucket-meta (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))]
                                (if (pos? (:object-count bucket-meta 0))
                                  (respond {:status 409
                                            :headers xml-content-type
                                            :body [:Error {}
                                                   [:Code {} "BucketNotEmpty"]
                                                   [:Resource {} (str \/ bucket)]
                                                   [:RequestId {} request-id]]})
                                  (do
                                    (sv/<? sv/S (k/update-in konserve [:bucket-meta] #(dissoc % bucket)))
                                    (sv/<? sv/S (k/update-in konserve [:version-meta] #(dissoc % bucket)))
                                    (respond {:status 204})))
                                (respond {:status 404
                                          :headers xml-content-type
                                          :body [:Error {}
                                                 [:Code {} "NoSuchBucket"]
                                                 [:Resource {} (str \/ bucket)]
                                                 [:RequestId {} request-id]]}))

                              (and (not-empty bucket) (not-empty object))
                              (if-let [bucket-meta (not-empty (sv/<? sv/S (k/get-in konserve [:bucket-meta bucket])))]
                                (respond (sv/<? sv/S (delete-object system request bucket object bucket-meta request-id)))
                                (respond {:status 404}
                                         :headers xml-content-type
                                         :body [:Error {}
                                                [:Code {} "NoSuchBucket"]
                                                [:Resource {} (str \/ bucket)]
                                                [:RequestId {} request-id]]))

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
            (log/warn x "exception in S3 handler")
            (respond {:status 500
                      :headers xml-content-type
                      :body [:Error {}
                             [:Code {} "InternalError"]
                             [:Resource {} (:uri request)]
                             [:RequestId {} request-id]]})))))))

(defn make-handler
  "Make an asynchronous, authenticated S3 ring handler.

  See [[s3-handler]] and [[s4.auth/authenticating-handler]]
  for what keys should be in `system`."
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



(comment
  "If you want a konserve file store, do this:"

  (def file-store (async/<!! (konserve.filestore/new-fs-store "foo" {:serializer (ser/fressian-serializer @read-handlers @write-handlers)})))

  "You need to use the custom read/write handlers for fressian, since by default
  fressian doesn't support sorted-map. This likely goes for any other
  konserve implementation, but I haven't looked.")