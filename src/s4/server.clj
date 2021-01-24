(ns s4.server
  (:import (java.time Clock)
           (java.net InetSocketAddress))
  (:require [s4.core :refer :all]
            [superv.async :as sv]
            [konserve.memory :as km]
            [s4.$$$$ :as $]
            [s4.auth :as auth]
            [s4.sqs :as sqs]
            [aleph.http :as http]
            [s4.util :as util]))

(defn make-server!
  "Launch a HTTP server that serves the S3 HTTP API.

  Arguments include:

  * `bind-address` A string, or an InetAddress; the address to bind
    to. Defaults to \"127.0.0.1\".
  * `port` A port number to bind to. Defaults to 0, which will bind
    to a random available port.
  * `konserve` An instance satisfying konserve.protocols/PEDNAsyncKeyValueStore.
    Will default to a in-memory store if not supplied.
  * `cost-tracker` A `s4.$$$$/ICostTracker` instance. Defaults to
    one that simulates default usage in us-west-2.
  * `reloadable?` If truthy, creates the HTTP handler such that if
    you reload namespaces s4.core or s4.auth, the changes are reflected
    in the running server. Default false.
  * `auth-store` An instance satisfying s4.auth.protocols/AuthStore.
    Defaults to a `s4.auth/AtomAuthStore` instance.
  * `hostname` The hostname to assign the server, default \"localhost\".
  * `request-id-prefix` A string to prepend to request IDs. Default nil.
  * `clock` A `java.time.Clock` to use for generating timestamps. Default
    is `(java.time.Clock/systemUTC)`
  * `sqs-server` An optional atom containing the system in use for simulated
    SQS queues; this must, at minimum, contain the key :queues, which is
    an atom containing a map of queue name -> queue. E.g. this should match
    what is returned by [[s4.sqs/make-server!]]. If not specified, SQS events
    are never triggered and can't be configured.
  * `enable-sqs?` If set to true, then SQS is assumed to be desired, and
    if `sqs-server` is nil, this will create a new SQS server, sharing the
    konserve, auth-store, bind-address, and reloadable? parameters, and
    binding to a random port.

  Return value is an atom, containing a map of keys:

  * `server` The aleph HTTP server.
  * `bind-address` The InetSocketAddress that was bound, including
    the port that was selected, if a random port was used.
  * `konserve` The konserve store instance.
  * `cost-tracker` The cost tracker instance.
  * `auth-store` The auth store instance. If you let this create the
    default store, you likely want to assoc your fake access keys into
    the atom contained by the AtomAuthStore."
  [{:keys [bind-address port konserve cost-tracker reloadable? auth-store hostname request-id-prefix clock sqs-server enable-sqs?]
    :or   {hostname     "localhost"
           clock        (Clock/systemUTC)
           bind-address "127.0.0.1"
           port         0}}]
  (let [bind-addr (InetSocketAddress. bind-address port)
        konserve (or konserve (sv/<?? sv/S (km/new-mem-store)))
        cost-tracker (or cost-tracker ($/cost-tracker konserve))
        auth-store (or auth-store (auth/->AtomAuthStore (atom {})))
        sqs-server (or sqs-server
                       (when enable-sqs?
                         (sqs/make-server! {:konserve     konserve
                                            :auth-store   auth-store
                                            :bind-address bind-address
                                            :reloadable?  reloadable?})))
        system {:konserve          konserve
                :cost-tracker      cost-tracker
                :hostname          hostname
                :request-counter   (atom 0)
                :request-id-prefix request-id-prefix
                :auth-store        auth-store
                :clock             clock
                :sqs-server        sqs-server}
        system-atom (atom system)
        handler (if reloadable?
                  (make-reloadable-handler system-atom)
                  (make-handler system))
        server (http/start-server (util/aleph-async-ring-adapter handler)
                                  {:socket-address bind-addr})]
    (swap! system-atom assoc
           :server server
           :bind-address (InetSocketAddress. bind-address (aleph.netty/port server)))
    system-atom))