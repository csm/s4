(ns s4.auth.protocols)

(defprotocol AuthStore
  (-get-secret-access-key [this access-key-id]
    "Return a channel that will yield the secret access key for the given access-key-id.
    Channel will return a Throwable on errors, should return nil if no secret key is found
    for that access-key-id."))