(ns memo.scheduler
  (:gen-class)
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

(defn- ttl-to-next-date [cron-exp]
  (let [now (time/now)
        next-dates (cron/forward-cron-sequence now cron-exp)
        next-date (first next-dates)]
    (time/in-millis (time/interval now next-date))))

(defn- setup-queues [ch]
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
        (debug (str "received expired message: " (String. payload "UTF-8")))
        (let [message (json/read-str (String. payload "UTF-8"))
              id (get meta "message-id")
              cron-exp (get message "cron-exp")
              ttl (ttl-to-next-date cron-exp)
              attributes {:message-id id :content-type "text/plain" :persistent true :expiration (str ttl)}]
          (amqp#basic/publish ch "" queue-name payload attributes))
        (catch Exception _
          (debug "next dates are consumed")))) {:auto-ack true}))

(defprotocol Scheduler
  (schedule [self dest cron message])
  (unschedule [self id])
  (schedules [self])
  (shutdown [self]))

(deftype AmqpScheduler [connection ch]
  Scheduler

  (schedule [self dest cron-exp message]
    (debug (str "dest: " dest " cron-exp: " cron-exp " message: " message))
    (try
      (let [id (str (random-uuid))
            ttl (ttl-to-next-date cron-exp)
            payload (json/write-str {:cron-exp cron-exp :message message})
            attributes {:message-id id :content-type "text/plain" :persistent true :expiration (str ttl)}]
        (amqp#basic/publish ch "" queue-name payload attributes)
        id)
      (catch Exception _
        (warn "cannot calculate next date for expression -> " (cron/explain-cron cron-exp) "(" cron-exp ")")
        (throw (Exception. "next date is in the past")))))

  (unschedule [self id]
    (debug (str "id: " id)))

  (schedules [self]
    (debug "schedules")
    [])

  (shutdown [self]
    (info "stopping scheduler...")
    (amqp#core/close connection)
    (info "stopped scheduler")))

(defn message-handler [ch meta ^bytes payload]
  (spy meta)
  (debug
    (str "received a message: " (String. payload "UTF-8")))
  (let [delivery-tag (:delivery-tag meta)]
    (future
      (Thread/sleep 4000)
      (debug "reject message " (:message-id meta))
      (amqp#basic/reject ch delivery-tag false))
    ))

(defn run []
  (info "starting scheduler...")

  (let [env (System/getenv)
        url (get env "CLOUDAMQP_URL" "amqp://guest:guest@rabbitmq")]
    (debug "connecting to " url)
    (let [connection (amqp#core/connect {:uri url})
          ch (amqp#channel/open connection)
          scheduler (AmqpScheduler. connection ch)]
      (setup-queues ch)
      (info "started scheduler")
      scheduler)))