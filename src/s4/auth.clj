(ns s4.auth
  (:require [byte-streams]
            [cemerick.uri :as uri]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [s4.auth.protocols :as s4p]
            [superv.async :as sv]
            [clojure.tools.logging :as log])
  (:import [java.time ZonedDateTime ZoneId LocalDateTime]
           [java.time.format DateTimeFormatter DateTimeParseException DateTimeFormatterBuilder SignStyle]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]
           [java.time.temporal ChronoField]
           [java.security MessageDigest]))

(defn hex
  [b]
  (string/join (map #(format "%02x" %) b)))

(defn canonical-request
  [request signed-headers]
  (with-out-str
    (println (-> (get request :request-method "GET") name string/upper-case))
    (println (-> (:uri request)
                 (uri/uri-decode)
                 (.split "/")
                 (->> (map uri/uri-encode)
                      (string/join \/))
                 (as-> uri (if (= \/ (first uri)) uri (str \/ uri)))))
    (if (:query-string request)
      (println (-> (:query-string request) (uri/query->map) (uri/map->query)))
      (println))
    (let [headers (->> (:headers request)
                       (filter #(or (signed-headers (key %))
                                    (string/starts-with? (key %) "x-amz-")))
                       (sort-by first))]
      (doseq [[header-name header-value] headers]
        (println (str header-name \: (string/trim header-value))))
      (println)
      (println (string/join \; (map first headers))))
    (print (get-in request [:headers "x-amz-content-sha256"]))))

(def UTC (ZoneId/of "UTC"))
(def datetime-formatter (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'"))
(def date-formatter (DateTimeFormatter/ofPattern "yyyyMMdd"))

(defn string-to-sign
  [^ZonedDateTime date region canonical-request-hash]
  (let [date (.withZoneSameInstant date UTC)]
    (with-out-str
      (println "AWS4-HMAC-SHA256")
      (println (.format date datetime-formatter))
      (println (str (.format date date-formatter) \/ region "/s3/aws4_request"))
      (print (hex canonical-request-hash)))))

(defn hmac-256
  [hkey content]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. (byte-streams/to-byte-array hkey) "HMacSHA256"))
    (.doFinal mac (byte-streams/to-byte-array content))))

(defn sha256
  [content]
  (let [md (MessageDigest/getInstance "SHA-256")]
    (.update md (byte-streams/to-byte-array content))
    (.digest md)))

(defn generate-signing-key
  [region date secret-key]
  (let [date-key (hmac-256 (str "AWS4" secret-key) (.format (.withZoneSameInstant date UTC) date-formatter))
        date-region-key (hmac-256 date-key region)
        date-region-service-key (hmac-256 date-region-key "s3")]
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

(defn parse-auth-header
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

(def RFC-1036-FORMATTER (-> (DateTimeFormatterBuilder.)
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

(def ASCTIME-FORMATTER (-> (DateTimeFormatterBuilder.)
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

(defn get-request-date
  [request]
  (if-let [amz-date (get-in request [:headers "x-amz-date"])]
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

(def +debug+ (= "true" (System/getProperty "s4.auth.debug")))

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

(defn authenticating-handler
  [handler {:keys [auth-store]}]
  (fn [request respond error]
    (async/go
      (log/debug :task ::authenticating-handler :phase :begin :request (pr-str request))
      (try
        (if-let [auth-header (debug "parse-auth-header:" (parse-auth-header (get-in request [:headers "authorization"])))]
          (if-let [secret-access-key (debug "get-secret-access-key:" (sv/<?? sv/S (s4p/-get-secret-access-key auth-store (get-in auth-header [:Credential :access-key]))))]
            (let [date (debug "get-request-date:" (get-request-date request))
                  canon-req (debug "canonical-request:" (canonical-request request (set (:SignedHeaders auth-header))))
                  s2s (debug "string-to-sign:" (string-to-sign date (get-in auth-header [:Credential :region]) (sha256 canon-req)))
                  key (debug "generate-signing-key:" (generate-signing-key (get-in auth-header [:Credential :region]) date secret-access-key))
                  mac (debug "mac:" (hex (hmac-256 key s2s)))]
              (if (= mac (:Signature auth-header))
                (handler request respond error)
                (respond {:status 401})))
            (respond {:status 401}))
          (respond {:status 401}))
        (catch Exception x (error x))))))

(defrecord AtomAuthStore [access-keys]
  s4p/AuthStore
  (-get-secret-access-key [_ access-key-id]
    (async/go
      (get @access-keys access-key-id))))
