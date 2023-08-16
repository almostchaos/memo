(ns memo.service
  (:require [org.httpkit.server :as hk])
  (:require [compojure.core :refer :all]
            [compojure.route :as route])
  (:require [ring.middleware.params]
            [ring.middleware.json :refer :all])
  (:require
    [taoensso.timbre :refer [trace debug info warn error spy]])
  (:require [memo.scheduler :as s]))

(defmacro on-term-signal [& handler]
  `(.addShutdownHook (Runtime/getRuntime)
                     (Thread. (fn []
                                (debug "sigterm captured")
                                ~@handler))))

(def url (get (System/getenv) "CLOUDAMQP_URL" "amqp://guest:guest@192.168.0.249"))

(defn -main [& args]
  (let [scheduler (s/run url)

        app (routes
              (POST "/schedule" [:as request]
                (let [body (:body request)]
                  (debug (s/schedule scheduler "webhooks" "2 * 2 * *" "bla"))
                  {:body body}))
              (POST "/unschedule" [:as request]
                (let [body (:body request)]
                  (debug (s/unschedule  scheduler "3456v345ty345vt5vtcbhdrtt"))
                  {:body body}))
              (GET "/schedules" [:as request]
                (let [body (:body request)]
                  (debug (s/schedules scheduler))
                  {:body {}}))
              (route/not-found "unknown endpoint"))

        shutdown-server (hk/run-server
                          (-> app
                              ring.middleware.json/wrap-json-body
                              ring.middleware.json/wrap-json-response)
                          {:port 8080})]
    (println "Started server.")
    (on-term-signal
      (info "Stopping server...")
      (shutdown-server)
      (s/shutdown scheduler)
      (info "Stopped server."))))