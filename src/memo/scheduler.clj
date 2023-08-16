(ns memo.scheduler
  (:gen-class)
  (:require [langohr.core :as amqp#core]
            [langohr.channel :as amqp#channel]
            [langohr.queue :as amqp#queue]
            [langohr.consumers :as amqp#consumer]
            [langohr.basic :as amqp#basic]
            [taoensso.timbre :refer [trace debug info warn error spy]]))

(def queue-name "memo-internal")

(defprotocol Scheduler
  (schedule [self dest cron message])
  (unschedule [self id])
  (schedules [self])
  (shutdown [self]))

(deftype AmqpScheduler [connection ch]
  Scheduler
  (schedule [self dest cron message]
    (debug (str "dest: " dest " cron: " cron " message: " message))
    (amqp#basic/publish ch "" queue-name message {:content-type "text/plain"}))
  (unschedule [self id]
    (debug (str "id: " id)))
  (schedules [self]
    (debug "schedules")
    [])
  (shutdown [self]
    (info "stopping scheduler...")
    (amqp#core/close connection)
    (info "stopped scheduler")))

(defn message-handler
  [ch {:keys [content-type delivery-tag] :as meta} ^bytes payload]
  (debug
    (str "received a message: " (String. payload "UTF-8") ", delivery tag: " delivery-tag ", content type: " content-type)))

(defn run [url]
  (info "starting scheduler...")
  (debug "using URL -> " url)
  (let [connection (amqp#core/connect {:uri url})
        ch (amqp#channel/open connection)
        scheduler (AmqpScheduler. connection ch)]
    (amqp#queue/declare ch queue-name {:exclusive false :auto-delete true})
    (amqp#consumer/subscribe ch queue-name message-handler {:auto-ack true})

    (info "started scheduler")
    scheduler))