(ns s4.sqs-test
  (:require [clojure.test :refer :all]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [s4.sqs :as sqs]
            [clojure.spec.alpha :as s]))

(def ^:dynamic *client* nil)

(use-fixtures
  :once
  (fn [f]
    (let [sqs-server (sqs/make-server! {})
          sqs-client (aws/client {:api                  :sqs
                                  :region               "us-west-2"
                                  :credentials-provider (creds/basic-credentials-provider {:access-key-id "ACCESSKEY"
                                                                                           :secret-access-key "SECRETACCESSKEY"})
                                  :endpoint-override    {:protocol "http"
                                                         :hostname "127.0.0.1"
                                                         :port (-> sqs-server deref :bind-address (.getPort))}})]
      (swap! (-> sqs-server deref :auth-store :access-keys) assoc "ACCESSKEY" "SECRETACCESSKEY")
      (try
        (binding [*client* sqs-client]
          (f))
        (finally
          (.close (-> sqs-server deref :server)))))))

(deftest test-queues
  (testing "that empty SQS queues can be listed"
    (let [response (aws/invoke *client* {:op :ListQueues})]
      (is (= {} response)))

    (let [create-response (aws/invoke *client* {:op      :CreateQueue
                                                :request {:QueueName "test-queues"}})
          list-response (aws/invoke *client* {:op :ListQueues})]
      (is (not (s/valid? ::anomalies/anomaly create-response)))
      (is (= {:QueueUrls [(:QueueUrl create-response)]}
             list-response))

      (testing "that we can send, receive, and delete messages"
        (let [send-response (aws/invoke *client* {:op      :SendMessage
                                                  :request {:QueueUrl    (:QueueUrl create-response)
                                                            :MessageBody "Hello, Queues!"}})]
          (is (not (s/valid? ::anomalies/anomaly send-response))))
        (let [recv-response (aws/invoke *client* {:op :ReceiveMessage
                                                  :request {:QueueUrl (:QueueUrl create-response)
                                                            :VisibilityTimeout 5}})]
          (is (not (s/valid? ::anomalies/anomaly recv-response)))
          (is (= 1 (count (:Messages recv-response))))
          (is (= "Hello, Queues!" (-> recv-response :Messages first :Body)))
          (let [delete-response (aws/invoke *client* {:op :DeleteMessage
                                                      :request {:QueueUrl (:QueueUrl create-response)
                                                                :ReceiptHandle (-> recv-response :Messages first :ReceiptHandle)}})]
            (is (not (s/valid? ::anomalies/anomaly delete-response))))
          (Thread/sleep 5000)
          (let [recv-response (aws/invoke *client* {:op :ReceiveMessage
                                                    :request {:QueueUrl (:QueueUrl create-response)}})]
            (is (= {} recv-response))))))))