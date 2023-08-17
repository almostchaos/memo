(ns memo.scheduler
  (:gen-class)
  (:require [langohr.core :as amqp#core]
            [langohr.channel :as amqp#channel]
            [langohr.queue :as amqp#queue]
            [langohr.consumers :as amqp#consumer]
            [langohr.basic :as amqp#basic]
            [taoensso.timbre :refer [trace debug info warn error spy]]))

(def queue-name "memo.internal")
(defn- setup-queues [ch]
  (amqp#queue/declare ch queue-name {:durable true
                                     :exclusive false
                                     :auto-delete false}))

(defprotocol Scheduler
  (schedule [self dest cron message])
  (unschedule [self id])
  (schedules [self])
  (shutdown [self]))

(deftype AmqpScheduler [connection ch]
  Scheduler

  (schedule [self dest cron message]
    (debug (str "dest: " dest " cron: " cron " message: " message))
    (let [id (str (random-uuid))
          attributes {:message-id id :content-type "text/plain" :persistent true :expiration "400"}]
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
    (if (< delivery-tag 100)
      (amqp#basic/reject ch delivery-tag true)
      (amqp#basic/ack ch delivery-tag))))

(defn run [url]
  (info "starting scheduler...")
  (debug "using URL -> " url)
  (let [connection (amqp#core/connect {:uri url})
        ch (amqp#channel/open connection)
        scheduler (AmqpScheduler. connection ch)]
    (setup-queues ch)
    (amqp#consumer/subscribe ch queue-name message-handler {:auto-ack false})

    (info "started scheduler")
    scheduler))