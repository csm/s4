(in-ns 's4.repl)

(require 'konserve.memory)
(require '[clojure.core.async :as async])
(def mem-store (async/<!! (konserve.memory/new-mem-store)))

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
(def cost-tracker (s4.$$$$/cost-tracker mem-store))

(swap! system assoc :konserve mem-store)
(swap! system assoc :hostname "localhost")
(swap! system assoc :request-counter (atom 0))
(swap! system assoc :cost-tracker cost-tracker)

(require 's4.core)
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

(aws/invoke s3-client {:op :DeleteBucket
                       :request {:Bucket "test"}})

