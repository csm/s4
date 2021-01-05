(ns s4.auth-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [ring.adapter.jetty :as jetty]
            [s4.auth :refer :all]
            s4.core
            [s4.util :refer :all]
            [cemerick.uri :as uri]
            [clojure.core.async :as async])
  (:import [java.net Socket]
           [com.google.common.io ByteStreams]
           [java.io PushbackReader ByteArrayInputStream ByteArrayOutputStream]
           [java.util.zip ZipFile]
           (java.util Arrays)))

(use-fixtures :once
  (fn [f]
    (System/setProperty "s4.auth.debug" "true")
    (f)))

(defmethod print-method (type (byte-array 0))
  [this w]
  (.write w "#bytes\"")
  (.write w (->> this
                 (map #(format "%02x" %))
                 (string/join)))
  (.write w "\""))

(defn uri-encode-except-slash
  [s]
  (let [[uri query] (string/split s #"\?" 2)
        uri (or (->> (split-path uri)
                     (map uri/uri-encode)
                     (string/join \/)
                     (not-empty))
                "/")]
    (if (empty? query)
      uri
      (str uri \? query))))

(let [echo-server (jetty/run-jetty (fn [request]
                                     (let [request (update request :body #(some-> % (ByteStreams/toByteArray)))]
                                       (log/debug "echo request:" request)
                                       {:status 200
                                        :body   (pr-str request)}))
                                   {:port 0 :join? false})
      port (-> echo-server (.getConnectors) first (.getLocalPort))]
  (defn parse-request-string
    [request]
    (let [request (string/replace request
                    #"(?ims)(GET|PUT|POST|DELETE) (.*) (HTTP/1.1.*)"
                    (fn [[_ method uri rest]]
                      (str method \space (uri-encode-except-slash uri) \space rest)))
          request (string/replace request #"([^\r])\n\n" "$1\r\n\r\n")
          request (string/replace request #"([^\r])\n" "$1\r\n")
          request (if (string/includes? request "\r\n\r\n")
                    request
                    (if (string/ends-with? request "\r\n")
                      (str request "\r\n")
                      (str request "\r\n\r\n")))]
      (log/debug "rewrote request:" (pr-str request))
      (with-open [socket (Socket. "127.0.0.1" port)]
        (let [out (.getOutputStream socket)]
          (.write out (.getBytes request "UTF-8"))
          (.flush out))
        (let [in (io/reader (.getInputStream socket))]
          (loop []
            (let [line (.readLine in)]
              (when-not (empty? line)
                (recur))))
          (let [result (edn/read {:readers {'bytes unhex}} (PushbackReader. in))]
            (log/debug "parsed request:" (pr-str result))
            (update result :body #(some-> % (ByteArrayInputStream.)))))))))

(def test-secret-access-key "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
(def test-auth-store (->AtomAuthStore (atom {"AKIDEXAMPLE" test-secret-access-key
                                             "AKIAIOSFODNN7EXAMPLE" "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})))

(deftest test-aws4-auth
  (testing "AWS signature generation"
    (let [response (promise)
          respond (partial deliver response)
          test-request (parse-request-string "GET /test.txt HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41
Range: bytes=0-9
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date: 20130524T000000Z")]
      ((authenticating-handler (fn [_ r _] (r :ok)) {:auth-store test-auth-store}) test-request respond nil)
      (is (= :ok @response)))
    (comment
      "jetty rejects this. FIXME"
      (let [response (promise)
            respond (partial deliver response)
            test-request (parse-request-string "PUT test$file.text HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Date: Fri, 24 May 2013 00:00:00 GMT
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd
x-amz-date: 20130524T000000Z
x-amz-storage-class: REDUCED_REDUNDANCY
x-amz-content-sha256: 44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072

Welcome to Amazon S3.")]
        ((authenticating-handler (fn [_ r _] (r :ok)) {:auth-store test-auth-store}) test-request respond nil)
        (is (= :ok @response))))
    (let [response (promise)
          respond (partial deliver response)
          test-request (parse-request-string "GET ?lifecycle HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543
x-amz-date: 20130524T000000Z
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")]
      ((authenticating-handler (fn [_ r _] (r :ok)) {:auth-store test-auth-store}) test-request respond nil)
      (is (= :ok @response)))
    (let [response (promise)
          respond (partial deliver response)
          test-request (parse-request-string "GET ?max-keys=2&prefix=J HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7
x-amz-date: 20130524T000000Z
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")]
      ((authenticating-handler (fn [_ r _] (r :ok)) {:auth-store test-auth-store}) test-request respond nil)
      (is (= :ok @response)))))

(deftest test-key-gen
  (testing "that we can generate signing keys correctly"
    (let [date "20150830"
          region "us-east-1"
          service "iam"
          key (hex (generate-signing-key region date service test-secret-access-key))]
      (is (= key "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9")))))

(defn gen-aws-tests*
  []
  (with-open [z (ZipFile. (io/file (io/resource "aws-sig-v4-test-suite.zip")))]
    (let [test-data (->> (.entries z)
                         (enumeration-seq)
                         (reduce (fn [m e]
                                   (if (.isDirectory e)
                                     m
                                     (let [[test-name data-type] (-> (.split (.getName e) "/")
                                                                     (last)
                                                                     (.split "\\."))]
                                       (if (and (not-empty data-type)
                                                (not= "readme" test-name)
                                                (not= "txt" data-type))
                                         (assoc-in m [test-name (keyword data-type)]
                                                     (slurp (.getInputStream z e)))
                                         m))))
                                 {}))]
      (map (fn [[test-name test]]
             `(deftest ~(symbol test-name)
                (let [req# (parse-request-string ~(get test :req))
                      auth-header# (parse-auth-header ~(get test :authz))
                      body# (:body req#)
                      body-sha256# (hex (sha256 (or body# (byte-array 0))))
                      date# (get-request-date req#)
                      canon-req# (canonical-request req# (set (:SignedHeaders auth-header#)) body-sha256#
                                                         (get-in auth-header# [:Credential :service]))
                      s2s# (string-to-sign date# (get-in auth-header# [:Credential :region]) (sha256 canon-req#)
                                                 (get-in auth-header# [:Credential :service]))
                      key# (generate-signing-key (get-in auth-header# [:Credential :region])
                                                 (get-in auth-header# [:Credential :date])
                                                 (get-in auth-header# [:Credential :service])
                                                 test-secret-access-key)
                      sig# (hex (hmac-256 key# s2s#))
                      test-request# (assoc-in req# [:headers "authorization"] ~(get test :authz))
                      response# (promise)
                      respond# (partial deliver response#)]
                  (is (= canon-req# ~(get test :creq)))
                  (is (= s2s# ~(get test :sts)))
                  (is (= sig# (get auth-header# :Signature)))
                  ((authenticating-handler (fn [_# r# _#] (r# :ok)) {:auth-store test-auth-store}) test-request# respond# nil)
                  (is (= :ok @response#)))))
           ; these tests have incorrect data in Amazon's own test suite.
           ; see https://forums.aws.amazon.com/thread.jspa?messageID=910843&#910843
           ; update this and the test data zip when/if Amazon fixes their data.
           ; Also, the test get-header-value-multiline is no longer valid, according
           ; to RFC 7230 https://tools.ietf.org/html/rfc7230#section-3.2.4; jetty
           ; rejects that request.
           (dissoc test-data "post-x-www-form-urlencoded-parameters" "post-x-www-form-urlencoded" "get-header-value-multiline")))))

(defmacro gen-aws-tests
  []
  `(do ~@(gen-aws-tests*)))

(gen-aws-tests)

(deftest test-chunked-signature
  (testing "that chunked signatures work"
    (let [body (let [out (ByteArrayOutputStream. 66824)
                     content (doto (byte-array 0x1000)
                               (Arrays/fill (byte \a)))]
                 (.write out (.getBytes "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n"))
                 (dotimes [_ 0x10]
                   (.write out content))
                 (.write out (byte \return))
                 (.write out (byte \newline))
                 (.write out (.getBytes "400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n"))
                 (.write out content 0 0x400)
                 (.write out (byte \return))
                 (.write out (byte \newline))
                 (.write out (.getBytes "0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n"))
                 (.write out (byte \return))
                 (.write out (byte \newline))
                 (.toByteArray out))
          request {:uri "/examplebucket/chunkObject.txt"
                   :request-method :put
                   :headers {"host" "s3.amazonaws.com"
                             "x-amz-date" "20130524T000000Z"
                             "x-amz-storage-class" "REDUCED_REDUNDANCY"
                             "authorization" "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
                             "x-amz-content-sha256" "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
                             "content-encoding" "aws-chunked"
                             "x-amz-decoded-content-length" "66560"
                             "content-length" "66824"}
                   :body (ByteArrayInputStream. body)}
          handler (authenticating-handler (fn [r respond error] (respond {:status 200}))
                                          {:auth-store (reify s4.auth.protocols/AuthStore
                                                         (-get-secret-access-key [_ id]
                                                           (let [chan (async/promise-chan)]
                                                             (when (= id "AKIAIOSFODNN7EXAMPLE")
                                                               (async/put! chan "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))
                                                             (async/close! chan)
                                                             chan)))})
          response (promise)
          error (promise)]
      (handler request #(do (deliver response %) (deliver error nil))
               #(do (deliver error %) (deliver response %)))
      @response
      (is (= {:status 200} @response)))))

