# s4

[![Clojars Project](https://img.shields.io/clojars/v/s4.svg)](https://clojars.org/s4)
[![cljdoc badge](https://cljdoc.org/badge/s4/s4)](https://cljdoc.org/d/s4/s4/CURRENT)

An implementation of the S3 API in clojure, using [konserve](https://github.com/replikativ/konserve) as
the backing store.

## Usage

```clojure
(def s4 (s4.core/make-server! {}))

(def port (-> @s4 :bind-address (.getPort)))
(swap! (-> @s4 :auth-store :access-keys) assoc "ACCESSKEY" "SECRETKEY")

(def s3-client (cognitect.aws.client.api/client
                 {:api :s3
                  :credentials-provider (cognitect.aws.credentials/basic-credentials-provider {:access-key-id "ACCESSKEY"
                                                                                               :secret-access-key "SECRETKEY"})
                  :endpoint-override {:protocol "http"
                                      :hostname "localhost"
                                      :port port}}))
(cognitect.aws.client.api/invoke s3-client {:op :ListBuckets})
```

Includes:

* Lots of the S3 HTTP API ([some of it still needs work](https://github.com/csm/s4/blob/master/TODO.md)).
* Support for AWS authentication, with pluggable authenticator providers.
* A cost estimator, which estimate costs for operations, data transfer, and data storage.
* A pluggable storage backend, so you can store things in memory for unit tests,
  on disk, or elsewhere.

## Why?

I wanted to run unit tests against an S3 facade, and that fakes3 thing both now
requires a license key, *and* it isn't compatible with the S3 API enough to make
the [cognitect client](https://github.com/cognitect-labs/aws-api) happy.

So I'm coding this out of spite.