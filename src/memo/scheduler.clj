(ns memo.scheduler
  (:gen-class)
  (:require [langohr.core :as amqp#core]
            [langohr.channel :as amqp#channel]
            [langohr.queue :as amqp#queue]
            [langohr.consumers :as amqp#consumer]
            [langohr.basic :as amqp#basic])
  (:require
    [taoensso.timbre :refer [trace  debug  info  warn  error  spy]]))