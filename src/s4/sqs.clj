(ns s4.sqs
  "Simulate SQS, for handling S3 notifications."
  (:require [aleph.http :as http]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :as log]
            [cemerick.uri :as uri]
            [konserve.memory :as km]
            [konserve.protocols :as kp]
            [s4.$$$$ :as $]
            [s4.auth :as auth]
            [s4.sqs.queues :refer :all]
            [s4.util :as util :refer [xml-content-type hex md5]]
            [superv.async :as sv]
            [clojure.string :as string]
            [clojure.spec.alpha :as s]
            [clojure.core.async :as async])
  (:import [com.google.common.io ByteStreams]
           [java.io ByteArrayInputStream DataOutputStream ByteArrayOutputStream]
           [java.time Clock ZonedDateTime ZoneOffset]
           [java.net InetSocketAddress]
           [java.util.concurrent DelayQueue Delayed TimeUnit LinkedBlockingQueue]
           [clojure.lang IDeref IHashEq Associative Counted]
           [java.time.format DateTimeFormatter]
           [java.util Base64 UUID AbstractMap$SimpleImmutableEntry]))

(defn ^:no-doc int-string-within?
  "Build a predicate that tells if the argument is a string,
  with a decimal representation within begin and end (inclusive)."
  [begin end]
  (fn [value]
    (and (string? value)
         (try (<= begin (Integer/parseInt value) end)
              (catch NumberFormatException _ false)))))

(s/def ::DelaySeconds (int-string-within? 0 900))
(s/def ::MaximumMessageSize (int-string-within? 1024 262144))
(s/def ::MessageRetentionPeriod (int-string-within? 60 1209600))
(s/def ::Policy string?)
(s/def ::ReceiveMessageWaitTimeSeconds (int-string-within? 0 20))
(s/def ::RedrivePolicy string?)
(s/def ::VisibilityTimeout (int-string-within? 0 43200))
(s/def ::KmsMasterKeyId string?)
(s/def ::KmsDataKeyReusePeriodSeconds (int-string-within? 60 86400))
(s/def ::FifoQueue #{"true" "false"})
(s/def ::ContentBasedDeduplication #{"true" "false"})

(s/def ::QueueAttributes (s/keys :opt-un [::DelaySeconds
                                          ::MaximumMessageSize
                                          ::MessageRetentionPeriod
                                          ::Policy
                                          ::ReceiveMessageWaitTimeSeconds
                                          ::RedrivePolicy
                                          ::VisibilityTimeout
                                          ::KmsMasterKeyId
                                          ::KmsDataKeyReusePeriodSeconds
                                          ::FifoQueue
                                          ::ContentBasedDeduplication]))

(s/def ::QueueAttributeName #{"All"
                              "ApproximateNumberOfMessages"
                              "ApproximateNumberOfMessagesDelayed"
                              "ApproximateNumberOfMessagesNotVisible"
                              "CreatedTimestamp"
                              "DelaySeconds"
                              "LastModifiedTimestamp"
                              "MaximumMessageSize"
                              "MessageRetentionPeriod"
                              "Policy"
                              "QueueArn"
                              "ReceiveMessageWaitTimeSeconds"
                              "RedrivePolicy"
                              "VisibilityTimeout"
                              "KmsMasterKeyId"
                              "KmsDataKeyReusePeriodSeconds"
                              "FifoQueue"
                              "ContentBasedDeduplication"})
(s/def ::QueueAttributeNames (s/coll-of ::QueueAttributeName))

(s/def ::MessageAttribute (fn [m]
                            (and (sequential? m)
                                 (= 3 (count m))
                                 (every? #(and (sequential? %)
                                               (= 2 (count %)))
                                         m)
                                 (some #(string/ends-with? (first %) ".Name") m)
                                 (some #(string/ends-with? (first %) ".Type") m)
                                 (some #(string/ends-with? (first %) ".Value") m))))
(s/def ::MessageAttributes (s/nilable (s/coll-of ::MessageAttribute)))

(defn message-body?
  [s]
  (and (string? s)
       (< (count s) (* 256 1024))
       (every? #(or (= % \tab)
                    (= % \newline)
                    (= % \return)
                    (<= (int \space) (int %) (int \ud7ff))
                    (<= (int \ue000) (int %) (int \ufffd)))
               s)))

(s/def ::MessageBody message-body?)

(s/def ::Tag (fn [t]
               (and (sequential? t)
                    (= 2 (count t))
                    (every? #(= 2 (count %)) t)
                    (some #(string/ends-with? (first %) ".Key") t)
                    (some #(string/ends-with? (first %) ".Value") t))))

(s/def ::Tags (s/nilable (s/coll-of ::Tag)))

(def default-queue-attributes
  {:DelaySeconds "0"
   :MaximumMessageSize "262144"
   :MessageRetentionPeriod "345600"
   :ReceiveMessageWaitTimeSeconds "0"
   :VisibilityTimeout "30"})

; request handling

(defmulti ^:no-doc handle-request
  (fn [system request request-id query]
    (get query "Action")))

(defmethod handle-request nil
  [_ _ _ _]
  (async/go {:status 400
             :headers xml-content-type
             :body [:Error [:Code "MissingAction"]]}))

(defmethod handle-request "CreateQueue"
  [{:keys [konserve server-info]} _ _ query]
  (sv/go-try
    sv/S
    (if-let [queue-name (get query "QueueName")]
      (if (some? (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name])))
        {:status 400
         :headers xml-content-type
         :body [:Error [:Code "QueueAlreadyExists"]]}
        (let [attributes (->> query
                              (filter #(re-matches #"Attribute\.[1-9]*[0-9]\.(?:Name|Value)" (key %)))
                              (group-by #(second (string/split (key %) #"\.")))
                              (vals)
                              (map #(vector (->> %
                                                 (filter (fn [[n]] (string/ends-with? n "Name")))
                                                 (first)
                                                 (second)
                                                 (keyword))
                                            (->> %
                                                 (filter (fn [[n]] (string/ends-with? n "Value")))
                                                 (first)
                                                 (second))))
                              (into {})
                              (s/conform ::QueueAttributes))
              tags (->> query
                        (filter #(re-matches #"Tag\.[1-9]*[0-9]\.(Key|Value)" (key %)))
                        (group-by #(second (string/split (key %) #"\.")))
                        (vals)
                        (map #(vector (->> %
                                           (filter (fn [[n]] (string/ends-with? n "Key")))
                                           (first)
                                           (second))
                                      (->> %
                                           (filter (fn [[n]] (string/ends-with? n "Value")))
                                           (first)
                                           (second))))
                        (into {}))]
          (if (= ::s/invalid attributes)
            {:status 400
             :headers xml-content-type
             :body [:Error [:Code "InvalidParameterValue"]]}
            (do
              (sv/<? sv/S (kp/-assoc-in konserve [:queue-meta queue-name] {:created (ZonedDateTime/now ZoneOffset/UTC)
                                                                           :attributes (merge default-queue-attributes attributes)
                                                                           :tags tags}))
              {:status 200
               :headers xml-content-type
               :body [:CreateQueueResponse
                      [:CreateQueueResult
                       [:QueueUrl (str "http://" (-> server-info deref :host)
                                       \: (-> server-info deref :port) \/ queue-name)]]]}))))
      {:status 400
       :headers xml-content-type
       :body [:Error [:Code "MissingParameter"]]})))

(defmethod handle-request "DeleteMessage"
  [{:keys [konserve queues]} _ request-id query]
  (sv/go-try
    sv/S
    (if-let [queue-name (some-> (get query "QueueUrl")
                                (uri/uri)
                                :path
                                (string/replace #"^/*" ""))]
      (if-let [queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
        (if-let [receipt-handle (get query "ReceiptHandle")]
          (let [queue (get-queue queues queue-name)
                result (sv/<? sv/S (remove! queue (->Message nil receipt-handle nil nil nil)))]
            (if result
              {:status 200
               :headers xml-content-type
               :body [:DeleteMessageResponse [:ResponseMetadata [:RequestId request-id]]]}
              {:status 400
               :headers xml-content-type
               :body [:ErrorResponse [:Error [:Code "ReceiptHandleIsInvalid"]]]}))
          {:status 400
           :headers xml-content-type
           :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request "DeleteQueue"
  [{:keys [konserve queues]} _ request-id query]
  (sv/go-try
    sv/S
    (if-let [queue-name (some-> (get query "QueueUrl")
                                (uri/uri)
                                :path
                                (string/replace #"^/" ""))]
      (let [[old new] (sv/<? sv/S (kp/-update-in konserve [:queue-meta]
                                                 #(dissoc % queue-name)))]
        (when-let [q (get @queues queue-name)]
          (stop q)
          (swap! queues dissoc queue-name))
        (if (= old new)
          {:status 400
           :headers xml-content-type
           :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]}
          {:status 200
           :headers xml-content-type
           :body [:DeleteQueueResponse
                  [:ResponseMetadata [:RequestId request-id]]]}))
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request "GetQueueAttributes"
  [{:keys [konserve queues]} _ request-id query]
  (async/go
    (if-let [queue-name (some-> (get query "QueueUrl")
                                (uri/uri)
                                :path
                                (string/replace #"^/*" ""))]
      (if-let [queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
        (let [attributes (->> query
                              (filter #(re-matches #"AttributeName\.[1-9]*[0-9]" (key %)))
                              (map #(vector (Integer/parseInt (second (string/split (key %) #"\.")))
                                            (val %)))
                              (sort-by first)
                              (map second)
                              (vec)
                              (s/conform ::QueueAttributeNames))
              attributes (if (neg? (.indexOf attributes "All"))
                           attributes
                           (disj (s/describe ::QueueAttributeName) "All"))
              values (->> attributes
                          (map (fn [attr-name]
                                 [attr-name
                                  (case attr-name
                                    "ApproximateNumberOfMessages" (str (if-let [q (get @queues queue-name)]
                                                                         (.size (.-messages q))
                                                                         0))
                                    "ApproximateNumberOfMessagesDelayed" (str (if-let [q (.get @queues queue-name)]
                                                                                (.size (.-delayed q))
                                                                                0))
                                    "ApproximateNumberOfMessagesNotVisible" (str (if-let [q (get @queues queue-name)]
                                                                                   (.size (.-hidden-messages q))
                                                                                   0))
                                    "CreatedTimestamp" (-> (:created queue-meta)
                                                           (.toInstant)
                                                           (.toEpochMilli)
                                                           (str))
                                    "LastModifiedTimestamp" (-> (or (:last-modified queue-meta)
                                                                    (:created queue-meta))
                                                                (.toInstant)
                                                                (.toEpochMilli)
                                                                (str))
                                    (get (:attributes queue-meta) (keyword attr-name)))]))
                          (remove #(nil? (second %)))
                          (into {}))]
          {:status 200
           :headers xml-content-type
           :body [:GetQueueAttributesResponse
                  (vec (concat [:GetQueueAttributesResult]
                               (map (fn [[k v]]
                                      [:Attribute [:Name k] [:Value v]])
                                    values)))]})
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request "GetQueueUrl"
  [{:keys [konserve server-info]} _ request-id query]
  (sv/go-try
    sv/S
    (if-let [queue-name (get query "QueueName")]
      (if-let [_queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
        {:status 200
         :headers xml-content-type
         :body [:GetQueueUrlResponse
                [:GetQueueUrlResult
                 [:QueueUrl (str "http://" (-> server-info deref :host)
                                 \: (-> server-info deref :port) \/ queue-name)]]
                [:ResponseMetadata [:RequestId request-id]]]}
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request "ListQueues"
  [{:keys [konserve server-info]} _ request-id query]
  (sv/go-try sv/S
             (let [queues (sv/<? sv/S (kp/-get-in konserve [:queue-meta]))
                   prefix (get query "QueueNamePrefix" "")]
               {:status 200
                :headers xml-content-type
                :body (vec (concat [:ListQueuesResponse]
                                   (vec (map #(vector :ListQueuesResult
                                                      [:QueueUrl (str (uri/uri (str "http://" (-> server-info deref :host)
                                                                                    \: (-> server-info deref :port))
                                                                               (key %)))])
                                             (filter #(string/starts-with? (key %) prefix)
                                                     queues)))
                                   [[:ResponseMetadata
                                     [:RequestId request-id]]]))})))

(defmethod handle-request "ListQueueTags"
  [{:keys [konserve server-info]} _ request-id query]
  (sv/go-try
    sv/S
    (if-let [queue-name (some-> (get query "QueueUrl")
                                (uri/uri)
                                :path
                                (string/replace #"^/*" ""))]
      (if-let [queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
        {:status 200
         :headers xml-content-type
         :body [:ListQueueTagsResponse
                (into [:ListQueueTagsResult]
                      (map (fn [[k v]] [:Tag [:Key k] [:Value v]])
                           (:tags queue-meta)))
                [:ResponseMetadata [:RequestId request-id]]]}
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request "ReceiveMessage"
  [{:keys [konserve queues]} _ request-id query]
  (sv/go-try
    sv/S
    (if-let [queue-name (some-> (get query "QueueUrl")
                                (uri/uri)
                                :path
                                (string/replace #"^/*" ""))]
      (if-let [queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
        (let [max-messages (->> (get query "MaxNumberOfMessages" "1")
                                (Integer/parseInt))
              visibility (->> (get query "VisibilityTimeout"
                                   (get-in queue-meta [:attributes :VisibilityTimeout]))
                              (Integer/parseInt))
              wait-time (->> (get query "WaitTimeSeconds" "0")
                             (Integer/parseInt))
              queue (get-queue queues queue-name)
              _ (log/debug :task ::handle-request :action "ReceiveMessage"
                           :max-messages max-messages
                           :visibility visibility
                           :wait-time wait-time)
              messages (sv/<? sv/S (poll! queue wait-time visibility max-messages))]
          {:status 200
           :headers xml-content-type
           :body [:ReceiveMessageResponse
                  (into
                    [:ReceiveMessageResult
                     [:ResponseMetadata [:RequestId request-id]]]
                    (map (fn [message]
                           (into [:Message
                                  [:MessageId (:message-id message)]
                                  [:ReceiptHandle (:receipt-handle message)]
                                  [:MD5OfBody (:body-md5 message)]
                                  [:Body (:body message)]]
                                 (map (fn [[k {:keys [value]}]]
                                        [:Attribute
                                         [:Name k]
                                         [:Value value]])
                                      (:attributes message))))
                         messages))]})
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request "SendMessage"
  [{:keys [konserve queues]} _ request-id query]
  (sv/go-try
    sv/S
    (try
      (if-let [queue-name (some-> (get query "QueueUrl")
                                  (uri/uri)
                                  :path
                                  (string/replace #"^/*" ""))]
        (if-let [queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
          (let [delay (->> (let [v (get query "DelaySeconds"
                                        (get-in queue-meta [:attributes :DelaySeconds]))]
                             (log/debug "DelaySeconds:" v)
                             v)
                           (s/conform ::DelaySeconds)
                           (Integer/parseInt))
                dbg (fn [tag x]
                      (log/debug tag x)
                      x)
                attributes (-> query
                               (->> (filter #(re-matches #"MessageAttribute\.[1-9]*[0-9]\.(Name|Type|Value)"
                                                         (key %)))
                                    (group-by #(second (string/split (key %) #"\.")))
                                    (vals)
                                    (dbg "attributes:")
                                    (s/conform ::MessageAttributes))
                               (as-> attrs
                                     (if (= ::s/invalid attrs)
                                       attrs
                                       (->> attrs
                                            (map (fn [a]
                                                   [(second (first (filter #(string/ends-with? % "Name") a)))
                                                    {:type (second (first (filter #(string/ends-with? % "Type") a)))
                                                     :value (second (first (filter #(string/ends-with? % "Value") a)))}]))
                                            (into {})))))
                body (s/conform ::MessageBody (get query "MessageBody"))]
            (if (or (= ::s/invalid attributes)
                    (= ::s/invalid body))
              {:status 400
               :headers xml-content-type
               :body [:ErrorResponse [:Error [:Code "InvalidQueryParameter"]]]}
              (let [body-md5 (hex (md5 body))
                    attributes-md5 (let [b (ByteArrayOutputStream.)
                                         out (DataOutputStream. b)]
                                     (doseq [[name {:keys [type value]}] (sort-by key attributes)]
                                       (.writeShort out 0)
                                       (.writeUTF out name)
                                       (.writeShort out 0)
                                       (.writeUTF out type)
                                       (if (string/includes? type "Binary")
                                         (do
                                           (.writeByte out 1)
                                           (let [bt (.decode (Base64/getDecoder) value)]
                                             (.writeInt out (count bt))
                                             (.write out bt)))
                                         (do
                                           (.writeByte out 2)
                                           (.writeShort out 0)
                                           (.writeUTF out value))))
                                     (hex (md5 (.toByteArray b))))
                    message-id (str (UUID/randomUUID))
                    message (->Message message-id
                                       message-id
                                       body
                                       body-md5
                                       attributes)
                    queue (get-queue queues queue-name)
                    retention (Integer/parseInt (get-in queue-meta [:attributes :MessageRetentionPeriod]))]
                (offer! queue message delay retention)
                {:status 200
                 :headers xml-content-type
                 :body [:SendMessageResponse
                        [:SendMessageResult
                         [:MD5OfMessageBody body-md5]
                         [:MD5OfMessageAttributes attributes-md5]
                         [:MessageId message-id]]
                        [:ResponseMetadata
                         [:RequestId request-id]]]})))
          {:status 400
           :headers xml-content-type
           :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]}))))

(defmethod handle-request "TagQueue"
  [{:keys [konserve server-info]} _ request-id query]
  (sv/go-try
    sv/S
    (if-let [queue-name (some-> (get query "QueueUrl")
                                (uri/uri)
                                :path
                                (string/replace #"^/*" ""))]
      (if-let [queue-meta (sv/<? sv/S (kp/-get-in konserve [:queue-meta queue-name]))]
        (let [tags (-> query
                       (->>
                         (filter #(re-matches #"Tag\.[1-9]*[0-9]\.(Key|Value)" (key %)))
                         (group-by #(second (string/split (key %) #"\.")))
                         (vals)
                         (s/conform ::Tags))
                       (as-> tags
                             (if (= ::s/invalid tags)
                               tags
                               (->> tags
                                    (map (fn [t]
                                           [(second (first (filter #(string/ends-with? (first %) ".Key") t)))
                                            (second (first (filter #(string/ends-with? (first %) ".Value") t)))]))
                                    (into {})))))]
          (if (= ::s/invalid tags)
            {:status 400
             :headers xml-content-type
             :body [:ErrorResponse [:Error [:Code "InvalidQueryParameter"]]]}
            (do
              (sv/<? sv/S (kp/-update-in konserve [:queue-meta queue-name :tags] #(merge % tags)))
              {:status 200
               :headers xml-content-type
               :body [:TagQueueResponse [:ResponseMetadata [:RequestId request-id]]]})))
        {:status 400
         :headers xml-content-type
         :body [:ErrorResponse [:Error [:Code "AWS.SimpleQueueService.NonExistentQueue"]]]})
      {:status 400
       :headers xml-content-type
       :body [:ErrorResponse [:Error [:Code "MissingParameter"]]]})))

(defmethod handle-request :default
  [_ _ _ _]
  (async/go {:status 500
             :headers xml-content-type
             :body [:ErrorResponse [:Error [:Code "NotImplemented"]]]}))

(defn sqs-handler
  [{:keys [request-id-prefix request-counter] :as system}]
  (fn [request respond _error]
    (async/go
      (try
        (let [request-id (str request-id-prefix (format "%016x" (swap! request-counter inc)))
              respond (fn respond-wrapper
                        [response]
                        (log/debug :task ::sqs-handler :phase :end :response (pr-str response))
                        (let [body (:body response)
                              body (if (and (= "application/xml" (get-in response [:headers "content-type"]))
                                            (sequential? body))
                                     (xml/emit-str (xml/sexp-as-element body) :encoding "UTF-8")
                                     body)]
                          (respond (assoc
                                     (assoc-in response [:headers "x-amz-request-id"] request-id)
                                     :body body))))
              query (merge
                      (if (= :get (:request-method request))
                        {}
                        (some-> (:body request) slurp uri/query->map))
                      (some-> (:query-string request) uri/query->map))]
          (log/debug :task ::sqs-handler :phase :begin
                     :query query)
          (respond (sv/<? sv/S (handle-request system request request-id query))))
        (catch Exception e
          (log/warn e "Exception in SQS handler")
          (respond {:status 500}))))))

(defn make-handler
  [system]
  (auth/authenticating-handler (sqs-handler system) system))

(defn make-reloadable-handler
  [system]
  (fn [request respond error]
    (let [body (some-> (:body request) (ByteStreams/toByteArray))
          _ (log/debug "request body:" body)
          request (assoc request :body (some-> body (ByteArrayInputStream.)))
          handler (sqs-handler @system)
          auth-handler (auth/authenticating-handler handler @system)]
      (auth-handler request respond error))))

(defn make-server!
  "Launch a HTTP server that serves the S3 HTTP API.

  Arguments include:

  * `bind-address` A string, or an InetAddress; the address to bind
    to. Defaults to \"127.0.0.1\".
  * `port` A port number to bind to. Defaults to 0, which will bind
    to a random available port.
  * `konserve` An instance satisfying konserve.protocols/PEDNAsyncKeyValueStore.
    Will default to a in-memory store if not supplied.
  * `cost-tracker` A `s4.$$$$/ICostTracker` instance. Defaults to
    one that simulates default usage in us-west-2.
  * `reloadable?` If truthy, creates the HTTP handler such that if
    you reload namespaces s4.core or s4.auth, the changes are reflected
    in the running server. Default false.
  * `auth-store` An instance satisfying s4.auth.protocols/AuthStore.
    Defaults to a `s4.auth/AtomAuthStore` instance.
  * `hostname` The hostname to assign the server, default \"localhost\".
  * `request-id-prefix` A string to prepend to request IDs. Default nil.
  * `clock` A `java.time.Clock` to use for generating timestamps. Default
    is `(java.time.Clock/systemUTC)`

  Return value is an atom, containing a map of keys:

  * `server` The aleph HTTP server.
  * `bind-address` The InetSocketAddress that was bound, including
    the port that was selected, if a random port was used.
  * `konserve` The konserve store instance.
  * `cost-tracker` The cost tracker instance.
  * `auth-store` The auth store instance. If you let this create the
    default store, you likely want to assoc your fake access keys into
    the atom contained by the AtomAuthStore."
  [{:keys [bind-address port konserve cost-tracker reloadable? auth-store hostname request-id-prefix clock]
    :or   {hostname     "localhost"
           clock        (Clock/systemUTC)
           bind-address "127.0.0.1"
           port         0}}]
  (let [bind-addr (InetSocketAddress. bind-address port)
        konserve (or konserve (sv/<?? sv/S (km/new-mem-store)))
        cost-tracker (or cost-tracker ($/cost-tracker konserve))
        auth-store (or auth-store (auth/->AtomAuthStore (atom {})))
        system {:konserve          konserve
                :cost-tracker      cost-tracker
                :hostname          hostname
                :request-counter   (atom 0)
                :request-id-prefix request-id-prefix
                :auth-store        auth-store
                :clock             clock
                :queues            (atom {})
                :server-info       (atom {})}
        system-atom (atom system)
        handler (if reloadable?
                  (make-reloadable-handler system-atom)
                  (make-handler system))
        server (http/start-server (util/aleph-async-ring-adapter handler)
                                  {:socket-address bind-addr})]
    (swap! (:server-info system) assoc :host bind-address)
    (swap! (:server-info system) assoc :port (aleph.netty/port server))
    (swap! system-atom assoc
           :server server
           :bind-address (InetSocketAddress. bind-address (aleph.netty/port server)))
    system-atom))