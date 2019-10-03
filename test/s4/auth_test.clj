(ns s4.auth-test
  (:require [clojure.test :refer :all]
            [s4.auth :refer :all]
            [s4.auth.protocols :as s4p]
            s4.core
            [clojure.core.async :as async]))

(deftest test-aws4-auth
  (let [auth-store (reify s4p/AuthStore
                     (-get-secret-access-key
                       [_ access-key]
                       (let [ch (async/promise-chan)]
                         (when (= access-key "AKIAIOSFODNN7EXAMPLE")
                           (async/put! ch "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))
                         (async/close! ch)
                         ch)))]
    (testing "AWS signature generation"
      (let [test-request {:aleph/request-arrived 194967281386879,
                          :aleph/keep-alive? true,
                          :remote-addr "127.0.0.1",
                          :headers {"range" "bytes=0-9",
                                    "host" "examplebucket.s3.amazonaws.com",
                                    "x-amz-date" "20130524T000000Z",
                                    "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                                    "authorization" "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"},
                          :server-port 56008,
                          :uri "/test.txt",
                          :server-name "localhost",
                          :query-string nil,
                          :body nil,
                          :scheme :http,
                          :request-method :get}]
        (is (= :ok @((s4.core/aleph-async-ring-adapter (authenticating-handler (fn [_ r _] (r :ok)) {:auth-store auth-store})) test-request))))
      (let [test-request {:aleph/request-arrived 201646135032056,
                          :aleph/keep-alive? true,
                          :remote-addr "127.0.0.1",
                          :headers {"host" "examplebucket.s3.amazonaws.com",
                                    "x-amz-date" "20130524T000000Z",
                                    "x-amz-content-sha256" "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
                                    "authorization" "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
                                    "date" "Fri, 24 May 2013 00:00:00 GMT",
                                    "x-amz-storage-class" "REDUCED_REDUNDANCY"},
                          :server-port 56982,
                          :uri "test$file.text",
                          :server-name "localhost",
                          :query-string nil,
                          :body nil,
                          :scheme :http,
                          :request-method :put}]
        (is (= :ok @((s4.core/aleph-async-ring-adapter (authenticating-handler (fn [_ r _] (r :ok)) {:auth-store auth-store})) test-request))))
      (let [test-request {:aleph/request-arrived 201646135032056,
                          :aleph/keep-alive? true,
                          :remote-addr "127.0.0.1",
                          :headers {"host" "examplebucket.s3.amazonaws.com",
                                    "x-amz-date" "20130524T000000Z",
                                    "x-amz-content-sha256" "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
                                    "authorization" "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
                                    "date" "Fri, 24 May 2013 00:00:00 GMT",
                                    "x-amz-storage-class" "REDUCED_REDUNDANCY"},
                          :server-port 56982,
                          :uri "test%24file.text",
                          :server-name "localhost",
                          :query-string nil,
                          :body nil,
                          :scheme :http,
                          :request-method :put}]
        (is (= :ok @((s4.core/aleph-async-ring-adapter (authenticating-handler (fn [_ r _] (r :ok)) {:auth-store auth-store})) test-request))))
      (let [test-request {:aleph/request-arrived 202695294437752,
                          :aleph/keep-alive? true,
                          :remote-addr "127.0.0.1",
                          :headers {"host" "examplebucket.s3.amazonaws.com",
                                    "x-amz-date" "20130524T000000Z",
                                    "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                                    "authorization" "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"},
                          :server-port 56982,
                          :uri "",
                          :server-name "localhost",
                          :query-string "lifecycle",
                          :body nil,
                          :scheme :http,
                          :request-method :get}]
        (is (= :ok @((s4.core/aleph-async-ring-adapter (authenticating-handler (fn [_ r _] (r :ok)) {:auth-store auth-store})) test-request))))
      (let [test-request {:aleph/request-arrived 202799311771206,
                          :aleph/keep-alive? true,
                          :remote-addr "127.0.0.1",
                          :headers {"host" "examplebucket.s3.amazonaws.com",
                                    "x-amz-date" "20130524T000000Z",
                                    "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                                    "authorization" "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"},
                          :server-port 56982,
                          :uri "",
                          :server-name "localhost",
                          :query-string "max-keys=2&prefix=J",
                          :body nil,
                          :scheme :http,
                          :request-method :get}]
        (is (= :ok @((s4.core/aleph-async-ring-adapter (authenticating-handler (fn [_ r _] (r :ok)) {:auth-store auth-store})) test-request)))))))