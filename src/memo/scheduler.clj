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

(defn- bytes-to-utf8-string [b]
  (String. b "UTF-8"))

(defn- next-date? [cron-exp]
  (try
    (first (cron/forward-cron-sequence (time/now) cron-exp))
    true
    (catch Exception e
      false)))

(defn- trigger-now? [cron-exp]
  (try
    (let [now (time/now)
          ref-time (time/minus now (time/minutes 1))
          trigger-time (first (cron/forward-cron-sequence ref-time cron-exp))]
      (time/within? (time/interval ref-time now) trigger-time))
    (catch Exception e false)))

(defn- setup-queues [connection]
  (let [ch (amqp#channel/open connection)]
    (amqp#queue/declare ch expired-queue-name {:durable true :exclusive false :auto-delete false})
    (amqp#exchange/fanout ch expired-exchange-name {:durable true})
    (amqp#queue/bind ch expired-queue-name expired-exchange-name)
    (amqp#queue/declare ch queue-name {:durable true :exclusive false :auto-delete false :arguments
                                       {"x-dead-letter-exchange" expired-exchange-name}})
    (amqp#consumer/subscribe
      ch
      expired-queue-name
      (fn [ch meta ^bytes payload]
        (let [message (json/read-str (bytes-to-utf8-string payload))
              cron-exp (get message "cron-exp")
              ttl (* 60 1000)
              attributes {:content-type "application/json" :persistent true :expiration (str ttl)}]
          (if (next-date? cron-exp)
            (do
              (amqp#basic/publish ch "" queue-name payload attributes)))
          (if (trigger-now? cron-exp)
            (do
              (info "fire schedule" message)))))
      {:auto-ack true})))

(defprotocol Scheduler
  (schedule [self dest cron message])
  (unschedule [self id])
  (unschedule-all [self])
  (shutdown [self]))

(deftype AmqpScheduler [connection ch]
  Scheduler

  (schedule [self dest cron-exp message]
    (debug (str "schedule events into " dest ", cron-exp: " cron-exp ", message: " message))
    (let [id (str (random-uuid))
          ttl (* 60 1000)
          payload (json/write-str {:id id :cron-exp cron-exp :message message})
          attributes {:content-type "application/json" :persistent true :expiration (str ttl)}]
      (amqp#basic/publish ch "" queue-name payload attributes)
      id))

  (unschedule [self id]
    (debug (str "id: " id))
    (let [temp-ch (amqp#channel/open connection)]
      (amqp#consumer/subscribe
        temp-ch queue-name
        (fn [ch meta ^bytes payload]
          (let [message (json/read-str (String. payload "UTF-8"))
                match? (= id (get message "id"))]
            (if match?
              (do
                (debug "unschedule" id)
                (amqp#basic/ack ch (:delivery-tag meta))
                (amqp#core/close ch)))))
        {:auto-ack false})))

  (unschedule-all [self]
    (amqp#queue/purge ch queue-name))

  (shutdown [self]
    (info "stopping scheduler...")
    (amqp#core/close connection)
    (info "stopped scheduler")))

(defn run []
  (info "starting scheduler...")

  (let [env (System/getenv)
        url (get env "CLOUDAMQP_URL" "amqp://guest:guest@rabbitmq")]
    (debug "connecting to " url)
    (let [connection (amqp#core/connect {:uri url})
          ch (amqp#channel/open connection)
          scheduler (AmqpScheduler. connection ch)]
      (setup-queues connection)
      (info "started scheduler")
      scheduler)))