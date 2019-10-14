(ns s4.util
  (:require [manifold.deferred :as d]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.string :as string])
  (:import [javax.crypto.spec SecretKeySpec]
           [javax.crypto Mac]
           [com.google.common.io ByteStreams]
           [java.io InputStream]
           [java.security MessageDigest]))

(def ^:no-doc xml-content-type {"content-type" "application/xml"})
(def ^:no-doc s3-xmlns "http://s3.amazonaws.com/doc/2006-03-01/")

(defn aleph-async-ring-adapter
  [handler]
  (fn [request]
    (let [deferred (d/deferred)]
      (handler request #(d/success! deferred %) #(d/error! deferred %))
      deferred)))

(defn ^:no-doc hex
  [b]
  (string/join (map #(format "%02x" %) b)))

(defn to-byte-array
  [x]
  (cond (bytes? x) x
        (string? x) (.getBytes x "UTF-8")
        (instance? InputStream x) (ByteStreams/toByteArray x)))

(defn ^:no-doc hmac-256
  [hkey content]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. (to-byte-array hkey) "HMacSHA256"))
    (.doFinal mac (to-byte-array content))))

(defn ^:no-doc sha256
  [content]
  (let [md (MessageDigest/getInstance "SHA-256")]
    (.update md (to-byte-array content))
    (.digest md)))

(defn ^:no-doc md5
  [content]
  (let [md (MessageDigest/getInstance "MD5")]
    (.update md (to-byte-array content))
    (.digest md)))
