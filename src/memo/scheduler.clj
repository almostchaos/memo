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

(defn- ttl-to-next-date [cron-exp]
  (let [now (time/now)
        next-dates (cron/forward-cron-sequence now cron-exp)
        next-date (first next-dates)]
    (time/in-millis (time/interval now next-date))))

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
        (try
          (let [message (json/read-str (bytes-to-utf8-string payload))
                cron-exp (get message "cron-exp")
                ttl (ttl-to-next-date cron-exp)
                attributes {:content-type "application/json" :persistent true :expiration (str ttl)}]
            (debug "re-schedule" message)
            (amqp#basic/publish ch "" queue-name payload attributes))
          (catch Exception _
            (debug "next dates are consumed"))))
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
    (try
      (let [id (str (random-uuid))
            ttl (ttl-to-next-date cron-exp)
            payload (json/write-str {:id id :cron-exp cron-exp :message message})
            attributes {:content-type "application/json" :persistent true :expiration (str ttl)}]
        (amqp#basic/publish ch "" queue-name payload attributes)
        id)
      (catch Exception e
        (warn "cannot calculate next date for expression -> " (cron/explain-cron cron-exp) "(" cron-exp ")")
        (throw (Exception. (str "next date is in the past for (" cron-exp ")" e))))))

  (unschedule [self id]
    (debug (str "id: " id))
    (amqp#consumer/subscribe
      ch queue-name
      (fn [ch meta ^bytes payload]
        (let [message (json/read-str (String. payload "UTF-8"))
              match? (= id (get message "id"))]
          (if match?
            (do
              (debug "unschedule" id)
              (amqp#basic/ack ch (:delivery-tag meta))
              (amqp#core/close ch)))))
      {:auto-ack false}))

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
      (amqp#core/automatic-recovery-enabled? connection)
      (setup-queues connection)
      (info "started scheduler")
      scheduler)))