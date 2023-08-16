(ns memo.scheduler
  (:gen-class)
  (:require [langohr.core :as amqp#core]
            [langohr.channel :as amqp#channel]
            [langohr.queue :as amqp#queue]
            [langohr.consumers :as amqp#consumer]
            [langohr.basic :as amqp#basic]
            [taoensso.timbre :refer [trace debug info warn error spy]]))

(defprotocol Scheduler
  (schedule [self dest cron message])
  (unschedule [self id])
  (schedules [self])
  (shutdown [self]))

(deftype AmqpScheduler [url]
  Scheduler
  (schedule [self dest cron message]
    (debug (str "dest: " dest " cron: " cron " message: " message)))
  (unschedule [self id]
    (debug (str "id: " id)))
  (schedules [self]
    (debug "schedules"))
  (shutdown [self]
    (info "Stopping scheduler...")
    (info "Stopped scheduler.")))

(defn run [url] (AmqpScheduler. url))