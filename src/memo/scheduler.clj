(ns memo.scheduler
  (:require [langohr.core :as amqp#core]
            [langohr.channel :as amqp#channel]
            [langohr.queue :as amqp#queue]
            [langohr.exchange :as amqp#exchange]
            [langohr.consumers :as amqp#consumer]
            [langohr.basic :as amqp#basic]
            [taoensso.timbre :refer [trace debug info warn error spy]]
            [clj-time.core :as time]
            [chronology.utils :as cron]
            [clojure.data.json :as json]))

(def queue-name "memo.internal")
(def expired-queue-name "memo.internal.expired")
(def expired-exchange-name "memo.internal.expired.exchange")

(defn- bytes-to-string
  ([bytes]
   (bytes-to-string bytes "UTF-8"))
  ([bytes encoding]
   (String. bytes encoding)))

(def poll-resolution (time/minutes 1))

(defn- trigger-next? [cron-exp]
  (try
    (first (cron/forward-cron-sequence (time/now) cron-exp))
    true
    (catch Exception e
      false)))

(defn- trigger-now? [cron-exp]
  (try
    (let [now (time/now)
          ref-time (time/minus now poll-resolution)
          trigger-time (first (cron/forward-cron-sequence ref-time cron-exp))]
      (time/within? (time/interval ref-time now) trigger-time))
    (catch Exception e false)))

(defn- ttl-to-next-poll []
  (let [now (time/now)
        next-poll-exact-time (time/plus now poll-resolution)
        spread (time/millis (rand-int 10000))
        next-poll-time (time/plus (time/floor next-poll-exact-time time/minute) spread)]
    (time/in-millis (time/interval now next-poll-time))))

(defn- setup-queues [ch]
  (amqp#queue/declare ch expired-queue-name {:durable     true
                                             :exclusive   false
                                             :auto-delete false
                                             :arguments   {"x-queue-type" "quorum"}})
  (amqp#exchange/fanout ch expired-exchange-name {:durable true})
  (amqp#queue/bind ch expired-queue-name expired-exchange-name)
  (amqp#queue/declare ch queue-name {:durable     true
                                     :exclusive   false
                                     :auto-delete false
                                     :arguments   {"x-queue-type"           "quorum"
                                                   "x-dead-letter-exchange" expired-exchange-name}}))

(defn amqp-scheduler [url target-exchange]
  (debug "starting schedule... ")
  (debug "connecting to " url)

  (let [connection (amqp#core/connect {:uri url})
        ch (amqp#channel/open connection)
        listener (fn [ch _ ^bytes payload]
                   (let [message (json/read-str (bytes-to-string payload))
                         cron-exp (get message "cron")]
                     (if (trigger-next? cron-exp)
                       (let [ttl (ttl-to-next-poll)
                             attributes {:content-type "application/json" :persistent true :expiration (str ttl)}]
                         (amqp#basic/publish ch "" queue-name payload attributes)))
                     (if (trigger-now? cron-exp)
                       (let [type (get message "type")
                             msg (get message "message")]
                         (info "fire schedule, send message" (str "'" msg "'") "to" type)
                         (amqp#basic/publish ch target-exchange type msg {:content-type "text/plain"})))))]


    (setup-queues ch)
    (amqp#consumer/subscribe ch expired-queue-name listener {:auto-ack true})
    (info "started scheduler")

    {:schedule
     (fn [dest cron-exp message]
       (debug (str "schedule events into " dest ", cron: " cron-exp ", message: " message))
       (let [id (str (random-uuid))
             ttl (time/in-millis poll-resolution)
             payload (json/write-str {:id id :type dest :cron cron-exp :message message})
             attributes {:content-type "application/json" :persistent true :expiration (str ttl)}]
         (amqp#basic/publish ch "" queue-name payload attributes)
         id))

     :unschedule
     (fn [id]
       (debug (str "id: " id))
       (let [temp-ch (amqp#channel/open connection)]
         (amqp#consumer/subscribe
           temp-ch queue-name
           (fn [ch meta ^bytes payload]
             (let [message (json/read-str (String. payload "UTF-8"))
                   delivery-tag (:delivery-tag meta)
                   match? (= id (get message "id"))]
               (if match?
                 (do
                   (debug "unschedule" id "delivery-tag" delivery-tag)
                   (amqp#basic/ack ch delivery-tag)
                   (Thread/sleep 200)
                   (amqp#core/close ch))
                 (if (amqp#channel/open? ch)
                   (amqp#basic/reject ch delivery-tag true)))))
           {:auto-ack false})))

     :unschedule-all
     (fn []
       (future
         (while (or (not (amqp#queue/empty? ch queue-name)) (not (amqp#queue/empty? ch queue-name)))
           (amqp#queue/purge ch expired-queue-name)
           (amqp#queue/purge ch queue-name)
           (Thread/sleep 1000))))

     :shutdown
     (fn []
       (info "stopping scheduler...")
       (amqp#core/close connection)
       (info "stopped scheduler"))}))

(defn run []
  (let [env (System/getenv)
        url (get env "CLOUDAMQP_URL" "amqp://guest:guest@rabbitmq")
        target-exchange (get env "TARGET_EXCHANGE" "")]
    (amqp-scheduler url target-exchange)))