(ns memo.service
  (:require [org.httpkit.server :as http]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.params]
            [ring.middleware.json :refer :all]
            [taoensso.timbre :refer [trace debug info warn error spy]]
            [memo.scheduler :as memo]))

(defmacro on-term-signal [& handler]
  `(.addShutdownHook (Runtime/getRuntime)
                     (Thread. (fn []
                                (debug "sigterm captured")
                                ~@handler))))

(defn -main [& args]
  (info "starting service...")
  (let [{:keys [schedule unschedule unschedule-all] shutdown-scheduler :shutdown} (memo/run)
        app (routes
              (POST "/schedule" [:as request]
                (let [body (:body request)
                      type (get body "type")
                      cron (get body "cron")
                      message (get body "message")
                      schedule-id (schedule type cron message)]
                  {:body {:id schedule-id}}))

              (POST "/unschedule" [:as request]
                (let [body (:body request)
                      id (get body "id")]
                  (unschedule id)
                  {:body nil}))

              (POST "/unschedule-all" []
                  (unschedule-all)
                  {:body nil})

              (route/not-found "unknown endpoint"))

        shutdown-server (http/run-server
                          (-> app
                              ring.middleware.json/wrap-json-body
                              ring.middleware.json/wrap-json-response)
                          {:port 8080})]

    (info "started service")
    (on-term-signal
      (info "stopping service...")
      (shutdown-server)
      (shutdown-scheduler)
      (info "stopped server"))))