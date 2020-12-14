(ns s4.auth
  (:require [cemerick.uri :as uri]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [s4.auth.protocols :as s4p]
            [s4.util :refer [hex sha256 hmac-256 unhex]]
            [superv.async :as sv]
            [clojure.tools.logging :as log])
  (:import [java.time ZonedDateTime LocalDateTime ZoneOffset]
           [java.time.format DateTimeFormatter DateTimeParseException DateTimeFormatterBuilder SignStyle]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]
           [java.time.temporal ChronoField]
           [java.security MessageDigest]
           [com.google.common.io ByteStreams]
           [java.io ByteArrayInputStream InputStream EOFException ByteArrayOutputStream]))

(defn ^:no-doc split-path
  "Similar to String.split, string/split, etc., but will
  include empty parts at the *end* of the string."
  [p]
  (reduce (fn [state ch]
            (cond
              (= ch \/)
              {:parts (conj (:parts state) (.toString (:buffer state)))
               :buffer (StringBuilder.)}

              (= ch ::end)
              (conj (:parts state) (.toString (:buffer state)))

              :else
              (do
                (.append (:buffer state) ch)
                state)))
          {:parts [] :buffer (StringBuilder.)}
          (concat p [::end])))

(defn ^:no-doc normalize-path
  [parts]
  (reduce
    (fn [{:keys [parts prev] :as state} part]
      (cond (and (= ::begin prev)
                 (not= "." part))
            {:parts (conj! parts part)
             :prev part}

            (and (= ::end part)
                 (empty? (:prev state)))
            (persistent! (conj! parts prev))

            (and (= ::end part)
                 (= ".." part))
            (persistent! (pop! parts))

            (= ::end part)
            (persistent! parts)

            (= ".." part)
            {:parts (pop! parts)
             :prev part}

            (and (not= "." part)
                 (not (empty? part)))
            {:parts (conj! parts part)
             :prev part}

            :else (assoc state :prev part)))

    {:parts (transient []) :prev ::begin}
    (concat parts [::end])))

(defn ^:no-doc canonical-request
  [request signed-headers content-sha256 service]
  (log/debug "canonical-request" (pr-str request) signed-headers content-sha256 service "URI:" (get request :uri))
  (with-out-str
    (println (-> (get request :request-method "GET") name string/upper-case))
    (if (= "s3" service)
      (println (as-> (get request :uri) u
                     (uri/uri-decode u)
                     (split-path u)
                     (map uri/uri-encode u)
                     (string/join \/ u)))
      (println (as-> (get request :uri) u
                     (uri/uri-decode u)
                     (split-path u)
                     (normalize-path u)
                     (map uri/uri-encode u)
                     (map #(string/replace % "%7E" "~") u) ; for some reason, ~ isn't encoded by AWS?
                     (string/join \/ u)
                     (if (empty? u) "/" u))))
    (if (string/blank? (:query-string request))
      (println)
      (println (-> (:query-string request)
                   (string/split #"&")
                   (seq)
                   (->> (map uri/split-param)
                        (map #(vec (map uri/uri-decode %))))
                   (uri/map->query)
                   (string/replace "%7E" "~"))))
    (let [headers (->> (:headers request)
                       (filter #(or (signed-headers (key %))
                                    (string/starts-with? (key %) "x-amz-")))
                       (sort-by first))]
      (doseq [[header-name header-value] headers]
        (println (str header-name \: (-> (string/trim header-value)
                                         (string/replace #"\s+" " ")))))
      (println)
      (println (string/join \; (map first headers))))
    (when content-sha256
      (print content-sha256))))

(def ^:no-doc UTC ZoneOffset/UTC)
(def ^:no-doc datetime-formatter (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'"))
(def ^:no-doc date-formatter (DateTimeFormatter/ofPattern "yyyyMMdd"))

(defn ^:no-doc string-to-sign
  [^ZonedDateTime date region canonical-request-hash service]
  (let [date (.withZoneSameInstant date UTC)]
    (with-out-str
      (println "AWS4-HMAC-SHA256")
      (println (.format date datetime-formatter))
      (println (str (.format date date-formatter) \/ region \/ service "/aws4_request"))
      (print (hex canonical-request-hash)))))

(defn ^:no-doc generate-signing-key
  [region date service secret-key]
  (let [date-key (hmac-256 (str "AWS4" secret-key) date)
        date-region-key (hmac-256 date-key region)
        date-region-service-key (hmac-256 date-region-key service)]
    (hmac-256 date-region-service-key "aws4_request")))

(s/def :auth-map/Credential (s/and string?
                                   #(re-matches #"([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^/]+)" %)))
(s/def :auth-map/SignedHeaders (s/and string?
                                      #(re-matches #"[^;]+(;[^;]+)*" %)))
(s/def :auth-map/Signature (s/and string?
                                  #(re-matches #"[0-9a-fA-F]{64}" %)))

(s/def ::auth-map (s/keys :req-un [:auth-map/Credential
                                   :auth-map/SignedHeaders
                                   :auth-map/Signature]))

(defn ^:no-doc parse-auth-header
  [header]
  (when-let [[mech auth-str] (some-> header (.split " " 2))]
    (when (= (string/lower-case mech) "aws4-hmac-sha256")
      (let [auth-map (->> (.split auth-str ",[ ]*")
                          (map #(let [[k v] (.split % "=" 2)]
                                  [(keyword k) v]))
                          (into {}))]
        (when (s/valid? ::auth-map auth-map)
          (-> auth-map
              (update :Credential (fn [cred]
                                    (let [[access-key date region service request] (.split cred "/")]
                                      {:access-key access-key
                                       :date date
                                       :region region
                                       :service service
                                       :request request})))
              (update :SignedHeaders #(vec (.split % ";")))))))))

(def ^:no-doc RFC-1036-FORMATTER (-> (DateTimeFormatterBuilder.)
                                     (.parseCaseInsensitive)
                                     (.parseLenient)
                                     (.optionalStart)
                                     (.appendText ChronoField/DAY_OF_WEEK {1 "Monday"
                                                                           2 "Tuesday"
                                                                           3 "Wednesday"
                                                                           4 "Thursday"
                                                                           5 "Friday"
                                                                           6 "Saturday"
                                                                           7 "Sunday"})
                                     (.appendLiteral ", ")
                                     (.optionalEnd)
                                     (.appendValue ChronoField/DAY_OF_MONTH 1 2 SignStyle/NOT_NEGATIVE)
                                     (.appendLiteral "-")
                                     (.appendText ChronoField/MONTH_OF_YEAR {1 "Jan"
                                                                             2 "Feb"
                                                                             3 "Mar"
                                                                             4 "Apr"
                                                                             5 "May"
                                                                             6 "Jun"
                                                                             7 "Jul"
                                                                             8 "Aug"
                                                                             9 "Sep"
                                                                             10 "Oct"
                                                                             11 "Nov"
                                                                             12 "Dec"})
                                     (.appendLiteral "-")
                                     (.appendValue ChronoField/YEAR 2)
                                     (.appendLiteral " ")
                                     (.appendValue ChronoField/HOUR_OF_DAY 2)
                                     (.appendLiteral ":")
                                     (.appendValue ChronoField/MINUTE_OF_HOUR 2)
                                     (.appendLiteral ":")
                                     (.appendValue ChronoField/SECOND_OF_MINUTE 2)
                                     (.appendLiteral " ")
                                     (.appendOffset "+HHMM", "GMT")
                                     (.toFormatter)))

(def ^:no-doc ASCTIME-FORMATTER (-> (DateTimeFormatterBuilder.)
                                   (.parseCaseInsensitive)
                                   (.parseLenient)
                                   (.optionalStart)
                                   (.appendText ChronoField/DAY_OF_WEEK {1 "Mon"
                                                                         2 "Tue"
                                                                         3 "Wed"
                                                                         4 "Thu"
                                                                         5 "Fri"
                                                                         6 "Sat"
                                                                         7 "Sun"})
                                   (.appendLiteral " ")
                                   (.appendValue ChronoField/DAY_OF_MONTH 1 2 SignStyle/NOT_NEGATIVE)
                                   (.appendLiteral " ")
                                   (.appendValue ChronoField/HOUR_OF_DAY 2)
                                   (.appendLiteral ":")
                                   (.appendValue ChronoField/MINUTE_OF_HOUR 2)
                                   (.appendLiteral ":")
                                   (.appendValue ChronoField/SECOND_OF_MINUTE 2)
                                   (.appendLiteral " ")
                                   (.appendValue ChronoField/YEAR 4)
                                   (.toFormatter)))

(defn ^:no-doc get-request-date
  [request]
  (if-let [amz-date (some-> (get-in request [:headers "x-amz-date"])
                            (string/split #",")
                            (first))]
    (-> (.parse datetime-formatter amz-date)
        (LocalDateTime/from)
        (ZonedDateTime/of UTC))
    (if-let [http-date (get-in request [:headers "date"])]
      (try (-> (.parse DateTimeFormatter/RFC_1123_DATE_TIME http-date)
               (LocalDateTime/from)
               (ZonedDateTime/of UTC))
           (catch DateTimeParseException _
             (try (-> (.parse RFC-1036-FORMATTER http-date)
                      (LocalDateTime/from)
                      (ZonedDateTime/of UTC))
                  (catch DateTimeParseException _
                    (-> (.parse ASCTIME-FORMATTER http-date)
                        (LocalDateTime/from)
                        (ZonedDateTime/of UTC)))))))))

(def ^:no-doc +debug+ (= "true" (System/getProperty "s4.auth.debug")))

(when +debug+
  (defmethod print-method (type (byte-array 0))
    [this w]
    (.write w "#bytes\"")
    (doseq [b this]
      (.write w (format "%02x" b)))
    (.write w "\"")))

(defn- debug
  [tag value]
  (when +debug+
    (log/debug tag (pr-str value)))
  value)

(defn read-hex-value
  [^InputStream input]
  (loop [result 0
         num 0]
    (let [ch (.read input)]
      (cond
        (< ch 0) (if (= num 0)
                   -1
                   (throw (EOFException.)))
        (= ch (int \;)) result
        (<= (int \0) ch (int \9)) (recur (bit-or (bit-shift-left result 4) (- ch (int \0))) (inc num))
        (<= (int \a) ch (int \f)) (recur (bit-or (bit-shift-left result 4) (+ 10 (- ch (int \a)))) (inc num))
        (<= (int \A) ch (int \F)) (recur (bit-or (bit-shift-left result 4) (+ 10 (- ch (int \A)))) (inc num))
        :else (throw (IllegalArgumentException. (str "invalid character " ch " in chunk header")))))))

(defn read-expect
  [^InputStream input ch-expect]
  (let [ch (.read input)]
    (cond
      (neg? ch) (throw (EOFException.))
      (not= (int ch-expect) ch) (throw (IllegalArgumentException. (str "expecting character " ch-expect " but read " (char ch))))
      :else true)))

(defn read-until
  ([input ch-end] (let [b (StringBuilder.)] (read-until input b ch-end)))
  ([input buffer ch-end]
   (let [ch (.read input)]
     (cond
       (neg? ch) (throw (EOFException.))
       (= (int ch-end) ch) (.toString buffer)
       :else (recur input (.append buffer (char ch)) ch-end)))))

(defn read-signature
 [^InputStream input]
 (doseq [ch "chunk-signature="]
   (read-expect input ch))
 (let [signature (unhex (read-until input \return))]
   (read-until input \newline)
   signature))

(defn decode-chunked-upload
  [mac-key seed-mac date auth-headers body]
  (let [in (ByteArrayInputStream. body)
        out (ByteArrayOutputStream.)]
    (loop [last-mac seed-mac]
      (let [chunk-length (debug "chunk length:" (read-hex-value in))]
        (if (= -1 chunk-length)
          [last-mac (.toByteArray out)]
          (let [expected-mac (debug "chunk signature:" (read-signature in))
                buffer (byte-array chunk-length)
                _ (debug "read bytes:" (.read in buffer))
                s2s (debug "chunk string-to-sign:" (str "AWS-HMAC-SHA256-PAYLOAD" \newline (.format date datetime-formatter) \newline
                                                        (.format date date-formatter) \/ (get-in auth-headers [:Credential :region])
                                                        \/ (get-in auth-headers [:Credential :service]) "/aws4_request" \newline
                                                        (hex last-mac) \newline (hex (sha256 "")) \newline
                                                        (hex (sha256 buffer))))
                mac (debug "computed chunk signature:" (hmac-256 mac-key s2s))]
            (when (not (MessageDigest/isEqual expected-mac mac))
              (throw (IllegalArgumentException. "invalid signature")))
            (read-expect in \return)
            (read-expect in \newline)
            (.write out buffer)
            (recur mac)))))))

(defn authenticating-handler
  "Create an asynchronous ring handler that authenticates
  requests before passing them on to `handler`.

  The `auth-store` key should be a [[s4.auth.protocols/AuthStore]]
  for resolving secret keys based on access keys."
  [handler {:keys [auth-store]}]
  (fn [request respond error]
    (async/go
      (log/debug :task ::authenticating-handler :phase :begin :request (pr-str request))
      (try
        (if-let [auth-header (debug "parse-auth-header:" (parse-auth-header (get-in request [:headers "authorization"])))]
          (if-let [secret-access-key (debug "get-secret-access-key:" (sv/<?? sv/S (s4p/-get-secret-access-key auth-store (get-in auth-header [:Credential :access-key]))))]
            (let [body (debug "body:" (some-> (:body request) (ByteStreams/toByteArray)))
                  body-sha256 (debug "content-sha256:" (-> (or body (byte-array 0)) sha256 hex))
                  sha256-header (debug "request-sha256:" (get-in request [:headers "x-amz-content-sha256"]))]
              (cond
                (= "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" sha256-header)
                (let [date (debug "get-request-date:" (get-request-date request))
                      canon-req (debug "canonical-request:" (canonical-request request (set (:SignedHeaders auth-header)) sha256-header (get-in auth-header [:Credential :service])))
                      s2s (debug "string-to-sign:" (string-to-sign date (get-in auth-header [:Credential :region]) (sha256 canon-req) (get-in auth-header [:Credential :service])))
                      key (debug "generate-signing-key:" (generate-signing-key (get-in auth-header [:Credential :region])
                                                                               (get-in auth-header [:Credential :date])
                                                                               (get-in auth-header [:Credential :service])
                                                                               secret-access-key))
                      seed-mac (debug "seed mac:" (hmac-256 key s2s))
                      [final-mac body] (decode-chunked-upload key seed-mac date auth-header body)]
                  (if (not (= (hex final-mac) (:Signature auth-header)))
                    (respond {:status 400})
                    (handler (assoc request :body (ByteArrayInputStream. body))
                             respond error)))

                (and (some? sha256-header)
                     (not= "UNSIGNED-PAYLOAD" sha256-header)
                     (not= body-sha256 sha256-header))
                (respond {:status 400})

                :else
                (let [date (debug "get-request-date:" (get-request-date request))
                      canon-req (debug "canonical-request:" (canonical-request request (set (:SignedHeaders auth-header)) sha256-header (get-in auth-header [:Credential :service])))
                      s2s (debug "string-to-sign:" (string-to-sign date (get-in auth-header [:Credential :region]) (sha256 canon-req) (get-in auth-header [:Credential :service])))
                      key (debug "generate-signing-key:" (generate-signing-key (get-in auth-header [:Credential :region])
                                                                               (get-in auth-header [:Credential :date])
                                                                               (get-in auth-header [:Credential :service])
                                                                               secret-access-key))
                      mac (debug "mac:" (hex (hmac-256 key s2s)))]
                  (if (= mac (:Signature auth-header))
                    (handler (if (some? body)
                               (assoc request :body (ByteArrayInputStream. body))
                               request)
                             respond error)
                    (respond {:status 401
                              :headers {"www-authenticate" "AWS4-HMAC-SHA256 realm=S4"}})))))
            (respond {:status 401
                      :headers {"www-authenticate" "AWS4-HMAC-SHA256 realm=S4"}}))
          (respond {:status 401
                    :headers {"www-authenticate" "AWS4-HMAC-SHA256 realm=S4"}}))
        (catch Exception x
          (log/error x "exception in authentication handler")
          (error x))))))

(defrecord AtomAuthStore [access-keys]
  s4p/AuthStore
  (-get-secret-access-key [_ access-key-id]
    (async/go
      (get @access-keys access-key-id))))
