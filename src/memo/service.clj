(ns memo.service
  (:require [org.httpkit.server :as hk])
  (:require [compojure.core :refer :all]
            [compojure.route :as route])
  (:require [ring.middleware.params]
            [ring.middleware.json :refer :all])
  (:require
    [taoensso.timbre :refer [trace debug info warn error spy]]))

(defmacro on-term-signal [& handler]
  `(.addShutdownHook (Runtime/getRuntime)
                     (Thread. (fn []
                                (debug "sigterm captured")
                                ~@handler))))

(defn -main [& args]
  (let [app (routes
              (POST "/schedule" [:as request]
                (let [body (:body request)]
                  (debug body)
                  {:body body}))
              (POST "/unschedule" [:as request]
                (let [body (:body request)]
                  (debug body)
                  {:body body}))
              (GET "/schedules" [:as request]
                (let [body (:body request)]
                  (debug body)
                  {:body body}))
              (route/not-found "unknown endpoint"))

        shutdown-server (hk/run-server
                          (-> app
                              ring.middleware.json/wrap-json-body
                              ring.middleware.json/wrap-json-response)
                          {:port 8080})]
    (println "Started server.")
    (on-term-signal
      (println "Stopping server...")
      (shutdown-server)
      (println "Stopped server."))))