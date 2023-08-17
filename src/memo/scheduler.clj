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
            [chronology.utils :as cron]))

(def queue-name "memo.internal")
(def expired-queue-name "memo.internal.expired")
(def expired-exchange-name "memo.internal.expired.exchange")
(defn- setup-queues [ch]
  (amqp#queue/declare ch expired-queue-name {:durable true :exclusive false :auto-delete false})
  (amqp#exchange/fanout ch expired-exchange-name {:durable true})
  (amqp#queue/bind ch expired-queue-name expired-exchange-name)
  (amqp#queue/declare ch queue-name {:durable true :exclusive false :auto-delete false :arguments
                                     {"x-dead-letter-exchange" expired-exchange-name}})
  (amqp#consumer/subscribe ch expired-queue-name
                           (fn [ch meta ^bytes payload]
                             (spy meta)
                             (debug
                               (str "received expired message: " (String. payload "UTF-8")))) {:auto-ack true}))

(defprotocol Scheduler
  (schedule [self dest cron message])
  (unschedule [self id])
  (schedules [self])
  (shutdown [self]))

(deftype AmqpScheduler [connection ch]
  Scheduler

  (schedule [self dest cron-exp message]
    (debug (str "dest: " dest " cron-exp: " cron-exp " message: " message))
    (let [id (str (random-uuid))
          now (time/now)
          next-date (first (cron/forward-cron-sequence now cron-exp))
          ttl (time/in-millis (time/interval now next-date))
          attributes {:message-id id :content-type "text/plain" :persistent true :expiration (str ttl)}]
      (amqp#basic/publish ch "" queue-name message attributes)
      id))

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

(defn run [url]
  (info "starting scheduler...")
  (debug "using URL -> " url)
  (let [connection (amqp#core/connect {:uri url})
        ch (amqp#channel/open connection)
        scheduler (AmqpScheduler. connection ch)]
    (setup-queues ch)
    ;(amqp#consumer/subscribe ch queue-name message-handler {:auto-ack false})

    (info "started scheduler")
    scheduler))