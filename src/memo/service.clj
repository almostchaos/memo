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

(def url (get (System/getenv) "CLOUDAMQP_URL" "amqp://guest:guest@192.168.0.142"))

(defn -main [& args]
  (info "starting service...")
  (let [scheduler (memo/run url)

        app (routes
              (POST "/schedule" [:as request]
                (let [body (:body request)
                      type (get body "type")
                      cron (get body "cron")
                      message (get body "message")
                      schedule-id (memo/schedule scheduler type cron message)]
                  {:body {:id schedule-id}}))
              (POST "/unschedule" [:as request]
                (let [body (:body request)
                      id (get body "id")]
                  (memo/unschedule scheduler id)
                  {:body nil}))
              (GET "/schedules" []
                  {:body (memo/schedules scheduler)})
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
      (memo/shutdown scheduler)
      (info "stopped server"))))