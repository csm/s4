# s4

An implementation of the S3 API in clojure, using [konserve](https://github.com/replikativ/konserve) as
the backing store.

## Why?

I wanted to run unit tests against an S3 facade, and that fakes3 thing both now
requires a license key, *and* it isn't compatible with the S3 API enough to make
the [cognitect client](https://github.com/cognitect-labs/aws-api) happy.

So yes, I'm coding this out of spite.