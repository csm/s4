(in-ns 's4.repl)

(import java.nio.ByteBuffer)

(defn buffer->seq
  [^ByteBuffer buf]
  (reify clojure.lang.ISeq
    (first [_]
      (.get (.duplicate buf)))
    (next [_]
      (when (< (.position buf) (dec (.limit buf)))
        (buffer->seq (.position (.duplicate buf) (inc (.position buf))))))
    (more [this]
      (.next this))
    (cons [this o]
      (clojure.lang.Cons. o this))

    clojure.lang.IPersistentCollection
    (count [_]
      (.limit buf))
    (empty [_]
      (.position (.slice buf) (.limit buf)))
    (equiv [_ o]
      (= buf o))

    (seq [this] this)))

(require '[clojure.string :as string])

(def printable (->> (range 128)
                    (filter #(and (not (Character/isISOControl %))
                                  (not (Character/isWhitespace %))))
                    (map #(vector % (char %)))
                    (into {})))

(defn hexdump
  [s]
  (cond (instance? ByteBuffer s) (hexdump (buffer->seq s))
        (bytes? s) (hexdump (seq s))
        (instance? clojure.lang.ISeq s) (let [s (partition-all 16 s)]
                                          (->> s
                                               (map-indexed
                                                 (fn [i bs]
                                                   (str (format "%08x" (* i 16))
                                                        "  "
                                                        (string/join " " (concat (map #(format "%02x" %) bs)
                                                                                 (repeat (- 16 (count bs)) "  ")))
                                                        "  "
                                                        (string/join (map #(printable (bit-and % 0xFF) \.) bs)))))
                                               (string/join \newline)))))


(require 'konserve.memory)
(require '[clojure.core.async :as async])
(def mem-store (async/<!! (konserve.memory/new-mem-store)))

(require 's4.core :reload)
(require 'konserve.filestore)
(require '[konserve.serializers :as ser])
(def file-store (async/<!! (konserve.filestore/new-fs-store "s4-test-data"
                                                            :serializer (ser/fressian-serializer @s4.core/read-handlers @s4.core/write-handlers))))

(def creds (atom {}))
(require 's4.auth.protocols)

(def system (atom {}))

(swap! system assoc :auth-store
       (reify s4.auth.protocols/AuthStore
         (-get-secret-access-key
           [_ id]
           (let [ch (async/promise-chan)]
             (when-let [key (get @creds id)]
               (async/put! ch key))
             (async/close! ch) ch))))

(require 's4.$$$$ :reload)
(def cost-tracker (s4.$$$$/cost-tracker file-store))

(swap! system assoc :konserve file-store)
(swap! system assoc :hostname "localhost")
(swap! system assoc :request-counter (atom 0))
(swap! system assoc :cost-tracker cost-tracker)

(def handler (s4.core/make-reloadable-handler system))

(require 'aleph.http)
(def server (aleph.http/start-server (s4.core/aleph-async-ring-adapter handler) {:port 0}))
(def port (aleph.netty/port server))

(require '[cognitect.aws.client.api :as aws])
(require '[cognitect.aws.credentials :as aws-creds])
(swap! creds assoc "AKIAIOSFODNN7EXAMPLE" "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
(def s3-client (aws/client {:api :s3
                            :credentials-provider (aws-creds/basic-credentials-provider {:access-key-id "AKIAIOSFODNN7EXAMPLE" :secret-access-key "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
                            :endpoint-override {:protocol :http :hostname "localhost" :port port}}))
(aws/invoke s3-client {:op :ListBuckets})

(aws/invoke s3-client {:op :CreateBucket
                       :request {:Bucket "test"}})
(aws/invoke s3-client {:op :ListBuckets})

(aws/invoke s3-client {:op :PutObject
                       :request {:Bucket "test"
                                 :Key "empty.txt"}})

(aws/invoke s3-client {:op :GetObject
                       :request {:Bucket "test"
                                 :Key "empty.txt"}})

(aws/invoke s3-client {:op :PutBucketVersioning
                       :request {:Bucket "test"
                                 :VersioningConfiguration {:Status "Enabled"}}})

(aws/invoke s3-client {:op :GetBucketVersioning
                       :request {:Bucket "test"}})

(aws/invoke s3-client {:op :PutObject
                       :request {:Bucket "test"
                                 :Key "file.txt"
                                 :Body (.getBytes "hello, world!")}})

(def ver1 (aws/invoke s3-client {:op :GetObject
                                 :request {:Bucket "test"
                                           :Key "file.txt"}}))
(slurp (:Body ver1))

(aws/invoke s3-client {:op :PutObject
                       :request {:Bucket "test"
                                 :Key "file.txt"
                                 :Body (.getBytes "hello again, world!")}})

(def ver2 (aws/invoke s3-client {:op :GetObject
                                 :request {:Bucket "test"
                                           :Key "file.txt"}}))
(slurp (:Body ver2))

(aws/invoke s3-client {:op :GetObject
                       :request {:Bucket "test"
                                 :Key "file.txt"
                                 :VersionId (:VersionId ver1)}})
(aws/invoke s3-client {:op :HeadObject
                       :request {:Bucket "test"
                                 :Key "file.txt"
                                 :VersionId (:VersionId ver1)}})

(aws/invoke s3-client {:op :ListObjectVersions
                       :request {:Bucket "test"}})

(aws/invoke s3-client {:op :DeleteBucket
                       :request {:Bucket "test"}})

(dotimes [i 100]
  (aws/invoke s3-client {:op :PutObject
                         :request {:Bucket "test"
                                   :Key (format "file%02x.txt" i)
                                   :Body (byte-array (repeat i (.byteValue i)))}}))

(s4.$$$$/get-cost-estimate cost-tracker (java.util.Date.))