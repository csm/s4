(ns s4.amz-test
  (:require [clojure.test :refer :all]
            [s4.test-util :refer :all])
  (:import (software.amazon.awssdk.auth.credentials StaticCredentialsProvider AwsBasicCredentials)
           (software.amazon.awssdk.services.s3 S3Client)
           (java.net URI)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.s3.model CreateBucketRequest ListObjectsRequest GetObjectRequest PutObjectRequest)
           (software.amazon.awssdk.core.sync RequestBody)))

(use-fixtures :each fixture)

(deftest test-amz-client
  (testing "that amazon client works against simulated server"
    (let [creds (StaticCredentialsProvider/create
                  (AwsBasicCredentials/create access-key secret-key))
          client ^S3Client (-> (S3Client/builder)
                               (.credentialsProvider creds)
                               (.region Region/US_WEST_2)
                               (.endpointOverride (URI. (str "http://localhost:" *port* "/")))
                               (.build))]
      (testing "list no buckets"
        (let [response (.listBuckets client)]
          (is (empty? (-> response (.buckets))))))
      (testing "creating a bucket works"
        (let [response (.createBucket client (-> (CreateBucketRequest/builder)
                                                 (.bucket "test")
                                                 (.build)))
              list-response (.listBuckets client)]
          (is (= ["test"] (map #(.name %) (.buckets list-response))))))
      (testing "listing empty bucket works"
        (let [list-objects (.listObjects client (-> (ListObjectsRequest/builder)
                                                    (.bucket "test")
                                                    (.build)))]
          (is (false? (.isTruncated list-objects)))
          (is (nil? (.marker list-objects)))
          (is (= "test" (.name list-objects)))
          (is (empty? (.contents list-objects)))))
      (testing "getting an invalid object fails"
        (try
          (.getObject client (-> (GetObjectRequest/builder)
                                 (.bucket "test")
                                 (.key "rabbit")
                                 (.build)))
          (is (nil? "failed, this should not have succeeded"))
          (catch Exception _ 'pass)))
      (testing "putting an object to a nonexistent bucket fails"
        (try
          (.putObject client (-> (PutObjectRequest/builder)
                                 (.bucket "rabbit")
                                 (.key "rabbit")
                                 (.build))
                      (RequestBody/fromString "foo"))
          (is (nil? "failed, this should not have succeeded"))
          (catch Exception _ 'pass)))
      (testing "putting an object succeeds"
        (.putObject client (-> (PutObjectRequest/builder)
                               (.bucket "test")
                               (.key "file")
                               (.build))
                    (RequestBody/fromString "foo")))
      (testing "listing existing objects succeeds"
        (let [response (.listObjects client (-> (ListObjectsRequest/builder)
                                                (.bucket "test")
                                                (.build)))]
          (is (= ["file"] (map #(.name %) (.contents response)))))))))