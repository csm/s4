(ns ^:no-doc s4.sqs.queues
    "Queues core."
  (:require [clojure.core.async :as async])
  (:import [java.util.concurrent DelayQueue LinkedBlockingQueue TimeUnit Delayed]
           [clojure.lang IDeref IHashEq Counted Associative IPersistentCollection Seqable ILookup MapEntry]))

; DelayQueue implementation
; could possibly do this with core.async, the only thing I can't
; figure out is how to remove queued values.

(deftype DelayBox [value delay created]
  IDeref
  (deref [_] value)

  Delayed
  (getDelay [_ unit]
    (let [delay-nanos (max 0 (- delay (- (System/currentTimeMillis) created)))]
      (.convert unit delay-nanos TimeUnit/MILLISECONDS)))

  (compareTo [this that]
    (compare (.getDelay this TimeUnit/MILLISECONDS)
             (.getDelay that TimeUnit/MILLISECONDS)))

  IHashEq
  (hasheq [_] (hash value))

  Object
  (equals [_ that]
    (and (instance? IDeref that)
         (= value @that))))

(defn ->delayed
  [value delay]
  (->DelayBox value (.convert TimeUnit/MILLISECONDS delay TimeUnit/SECONDS)
              (System/currentTimeMillis)))

(defprotocol IQueues
  (start [this] "Starts queue processor, if not already started.")
  (stop [this] "Stops queue processor.")
  (offer! [this value delay retention]
    "Place a value on the queue, with a delay and a retention period.
    If delay is non-zero, the value will wait that long before appearing in
    the queue. The value is automatically removed from the queue once
    the retention duration elapses.")
  (poll! [this timeout hide-duration max-messages]
    "Poll the queue for messages. Returns a channel that will
    yield a vector of messages (up to 10) if any become available
    within the given timeout. The hide-duration parameter controls
    how long the messages taken are hidden, before they are removed
    entirely from the queue.")
  (remove! [this value]
    "Remove value from the queue."))

(defrecord Queues [messages delayed hidden-messages state]
  IQueues
  (start [_]
    (when (compare-and-set! state nil ::running)
      (async/thread
        (loop []
          (let [message (.take delayed)]
            (when-not (= ::closing @message)
              (.offer messages @message))
            (when (= ::running @state)
              (recur)))))
      (async/thread
        (loop []
          (let [message (.take hidden-messages)]
            (when-not (= ::closing @message)
              (.offer messages @message)))
          (when (= ::running @state)
            (recur))))))

  (stop [_]
    (reset! state ::closed)
    (.offer hidden-messages (->delayed ::closing 0))
    (.offer delayed (->delayed ::closing 0)))

  (offer! [_ value delay retention]
    (if (zero? delay)
      (.offer messages value)
      (.offer delayed (->delayed value delay)))
    (async/go
      (async/<! (async/timeout (.convert TimeUnit/MILLISECONDS
                                         retention TimeUnit/SECONDS)))
      (async/<!
        (async/thread
          (.remove messages value)
          (.remove delayed (->delayed value 0))
          (.remove hidden-messages (->delayed value 0))))))

  (poll! [_ timeout hide-duration max-messages]
    (async/thread
      (let [max-messages (min 10 max-messages)]
        (if-let [message (.poll messages timeout TimeUnit/SECONDS)]
          (do
            (.offer hidden-messages (->delayed message hide-duration))
            (if (< 1 max-messages)
              (loop [messages [message]]
                (if-let [message (.poll messages)]
                  (do
                    (.offer hidden-messages (->delayed message hide-duration))
                    (if (< (count messages) max-messages)
                      (recur (conj messages message))
                      messages))
                  messages))
              [message]))
          []))))

  (remove! [_ value]
    (async/thread
      (or (.remove messages value)
          (.remove delayed (->delayed value 0))
          (.remove hidden-messages (->delayed value 0))))))

(defn ^:no-doc get-queue
  "Fetch (or create) the queue with name."
  [queues name]
  (doto (get (swap! queues update name (fn [q]
                                         (or q (->Queues (LinkedBlockingQueue.)
                                                         (DelayQueue.)
                                                         (DelayQueue.)
                                                         (atom nil)))))
             name)
    (start)))

; messages, equality by receipt-id only!

(deftype Message [message-id receipt-handle body body-md5 attributes]
  Associative
  (containsKey [_ k] (some? (#{:message-id :receipt-handle :body :body-md5 :attributes} k)))

  (entryAt [_ k]
    (case k
      :message-id     (MapEntry. k message-id)
      :receipt-handle (MapEntry. k receipt-handle)
      :body           (MapEntry. k body)
      :body-md5       (MapEntry. k body-md5)
      :attributes     (MapEntry. k attributes)
      nil))

  (assoc [_ k v]
    (assoc {:message-id message-id
            :receipt-handle receipt-handle
            :body body
            :body-md5 body-md5
            :attributes attributes}
      k v))

  ILookup
  (valAt [this key] (.valAt this key nil))

  (valAt [this key not-found]
    (if-let [e (.entryAt this key)]
      (val e)
      not-found))

  IPersistentCollection
  (count [_] 5)

  (cons [this v]
    (let [[k v] v]
      (.assoc this k v)))

  (empty [_] {})

  (equiv [this that]
    (and (instance? (type this) that)
         (= receipt-handle (.-receipt-handle that))))

  Seqable
  (seq [this] (iterator-seq (.iterator this)))

  Iterable
  (iterator [_]
    (.iterator [(MapEntry. :message-id message-id)
                (MapEntry. :receipt-handle receipt-handle)
                (MapEntry. :body body)
                (MapEntry. :body-md5 body-md5)
                (MapEntry. :attributes attributes)]))

  Counted
  Object
  (equals [this that]
    (and (instance? (type this) that)
         (= receipt-handle (.-receipt-handle that)))))
