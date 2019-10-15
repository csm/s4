(ns s4.core-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [s4.core :as s4]
            [konserve.protocols :as kp]
            [clojure.core.async :as async])
  (:import [java.time Clock ZoneOffset Instant]
           [java.util Date]))

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

(use-fixtures
  :each
  (fn [f]
    (let [s4 (s4/make-server! {:clock (static-clock secs)})]
      (swap! (-> @s4 :auth-store :access-keys) assoc access-key secret-key)
      (try
        (binding [*s4* s4
                  *port* (-> @s4 :bind-address (.getPort))]
          (f))
        (finally
          (.close (:server @s4)))))))

(deftest test-s3-api
  (let [client (aws/client {:api :s3
                            :credentials-provider (creds/basic-credentials-provider {:access-key-id access-key
                                                                                     :secret-access-key secret-key})
                            :endpoint-override {:protocol "http"
                                                :hostname "localhost"
                                                :port *port*}
                            :region "us-west-2"})]
    (testing "that we can do bucket operations"
      (is (= {:Buckets [] :Owner {:ID "S4" :DisplayName "You Know, for Data"}}
             (aws/invoke client {:op :ListBuckets})))
      (let [r (aws/invoke client {:op :HeadBucket :request {:Bucket "rabbit"}})]
        (is (s/valid? ::anomalies/anomaly r))
        (is (= ::anomalies/not-found (::anomalies/category r))))
      (is (= {:Location "/test"} (aws/invoke client {:op :CreateBucket
                                                     :request {:Bucket "test"}})))
      (is (= {:Buckets [{:Name "test" :CreationDate (Date. (* @secs 1000))}]
              :Owner {:ID "S4" :DisplayName "You Know, for Data"}}
             (aws/invoke client {:op :ListBuckets})))
      (is (= {} (aws/invoke client {:op :HeadBucket :request {:Bucket "test"}}))))

    (testing "that we can do object operations"
      (let [r (aws/invoke client {:op :ListObjectsV2 :request {:Bucket "rabbit"}})]
        (is (s/valid? ::anomalies/anomaly r))
        (is (= ::anomalies/not-found (::anomalies/category r))))
      (is (= {:Prefix "" :Delimiter "" :Name "test" :MaxKeys 1000 :IsTruncated false :KeyCount 0}
             (aws/invoke client {:op :ListObjectsV2 :request {:Bucket "test"}})))
      (is (= {:ETag "d41d8cd98f00b204e9800998ecf8427e"}
             (aws/invoke client {:op :PutObject :request {:Bucket "test"
                                                          :Key "empty.txt"}})))
      (swap! secs inc)
      (is (= {:ETag "6cd3556deb0da54bca060b4c39479839"}
             (aws/invoke client {:op :PutObject :request {:Bucket "test"
                                                          :Key "hello.txt"
                                                          :ContentType "text/plain"
                                                          :Body (.getBytes "Hello, world!")}})))
      (is (= {:Prefix "" :Delimiter "" :Name "test" :MaxKeys 1000 :IsTruncated false :KeyCount 2
              :Contents [{:Key "empty.txt"
                          :LastModified (Date. (* epoch 1000))
                          :ETag "\"d41d8cd98f00b204e9800998ecf8427e\""
                          :Size 0
                          :StorageClass "STANDARD"}
                         {:Key "hello.txt"
                          :LastModified (Date. (* (inc epoch) 1000))
                          :ETag "\"6cd3556deb0da54bca060b4c39479839\""
                          :Size (count "Hello, world!")
                          :StorageClass "STANDARD"}]}
             (aws/invoke client {:op :ListObjectsV2 :request {:Bucket "test"}})))
      (swap! secs inc)
      (is (= {:ETag "340788bfefcd39a4f2108f11bb9f2101"}
             (aws/invoke client {:op :PutObject :request {:Bucket "test"
                                                          :Key "encode$file.text"
                                                          :Body (.getBytes "Hello, URL encoding!")}})))
      (is (= {:Prefix "" :Delimiter "" :Name "test" :MaxKeys 1000 :IsTruncated false :KeyCount 3
              :Contents [{:Key "empty.txt"
                          :LastModified (Date. (* epoch 1000))
                          :ETag "\"d41d8cd98f00b204e9800998ecf8427e\""
                          :Size 0
                          :StorageClass "STANDARD"}
                         {:Key "encode$file.text"
                          :LastModified (Date. (* (+ epoch 2) 1000))
                          :ETag "\"340788bfefcd39a4f2108f11bb9f2101\""
                          :Size (count "Hello, URL encoding!")
                          :StorageClass "STANDARD"}
                         {:Key "hello.txt"
                          :LastModified (Date. (* (inc epoch) 1000))
                          :ETag "\"6cd3556deb0da54bca060b4c39479839\""
                          :Size (count "Hello, world!")
                          :StorageClass "STANDARD"}]}
             (aws/invoke client {:op :ListObjectsV2 :request {:Bucket "test"}})))
      (is (= {:ETag "\"d41d8cd98f00b204e9800998ecf8427e\""
              :ContentType "binary/octet-stream"
              :LastModified (Date. (* epoch 1000))
              :ContentLength 0
              :Metadata {}
              :AcceptRanges "bytes"}
             (aws/invoke client {:op :HeadObject :request {:Bucket "test"
                                                           :Key "empty.txt"}})))
      (is (= {:ETag "\"6cd3556deb0da54bca060b4c39479839\""
              :ContentType "text/plain"
              :LastModified (Date. (* (inc epoch) 1000))
              :ContentLength (count "Hello, world!")
              :Metadata {}
              :AcceptRanges "bytes"}
             (aws/invoke client {:op :HeadObject :request {:Bucket "test"
                                                           :Key "hello.txt"}})))
      (is (= {:ETag "\"340788bfefcd39a4f2108f11bb9f2101\""
              :ContentType "application/octet-stream"
              :LastModified (Date. (* (+ 2 epoch) 1000))
              :ContentLength (count "Hello, URL encoding!")
              :Metadata {}
              :AcceptRanges "bytes"}
             (aws/invoke client {:op :HeadObject :request {:Bucket "test"
                                                           :Key "encode$file.text"}})))
      (let [obj (aws/invoke client {:op :GetObject :request {:Bucket "test"
                                                             :Key "empty.txt"}})]
        (is (= {:ETag "\"d41d8cd98f00b204e9800998ecf8427e\""
                :ContentType "binary/octet-stream"
                :LastModified (Date. (* epoch 1000))
                :ContentLength 0
                :Metadata {}
                :AcceptRanges "bytes"
                :Body nil}
               obj)))
      (let [obj (aws/invoke client {:op :GetObject :request {:Bucket "test"
                                                             :Key "hello.txt"}})]
        (is (= {:ETag "\"6cd3556deb0da54bca060b4c39479839\""
                :ContentType "text/plain"
                :LastModified (Date. (* (inc epoch) 1000))
                :ContentLength (count "Hello, world!")
                :Metadata {}
                :AcceptRanges "bytes"}
               (dissoc obj :Body)))
        (is (= "Hello, world!" (slurp (:Body obj)))))
      (let [obj (aws/invoke client {:op :GetObject :request {:Bucket "test"
                                                             :Key "encode$file.text"}})]
        (is (= {:ETag "\"340788bfefcd39a4f2108f11bb9f2101\""
                :ContentType "application/octet-stream"
                :LastModified (Date. (* (+ 2 epoch) 1000))
                :ContentLength (count "Hello, URL encoding!")
                :Metadata {}
                :AcceptRanges "bytes"}
               (dissoc obj :Body)))
        (is (= "Hello, URL encoding!" (slurp (:Body obj)))))
      (is (= {}
             (aws/invoke client {:op :DeleteObject
                                 :request {:Bucket "test"
                                           :Key "rabbit"}})))
      (is (= {}
             (aws/invoke client {:op :DeleteObject
                                 :request {:Bucket "test"
                                           :Key "empty.txt"}})))
      (is (= {}
             (aws/invoke client {:op :DeleteObject
                                 :request {:Bucket "test"
                                           :Key "hello.txt"}})))
      (is (= {}
             (aws/invoke client {:op :DeleteObject
                                 :request {:Bucket "test"
                                           :Key "encode$file.text"}})))
      (is (empty? (async/<!! (kp/-get-in (:konserve @*s4*) [:blobs "test"]))))

      (is (= {} (aws/invoke client {:op :DeleteBucket
                                    :request {:Bucket "test"}}))))))
