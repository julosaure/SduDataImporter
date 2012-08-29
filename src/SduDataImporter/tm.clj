(ns SduDataImporter.tm
  (:require clojure.java.io)
  (:require [clojure-solr :as cs])
  (:require clojure.string)
  (:use (chisel instances lda))
  (:import [java.io File]))

;; GLOBALS




;; UTILS

(def connection (clojure-solr/connect "http://127.0.0.1:8983/solr"))



;; FUNCTIONS

(defn rowToDoc [row]
  [(row "activity_row_id") (row "description")])
   
(defn querySolr []
  (cs/with-connection connection
    (cs/search "*:*" "rows" 5000 "fl" "description,activity_row_id")))


(defn tmx []
  (let [doc1 (map rowToDoc (querySolr))
        docs (apply hash-map (flatten doc1))
        instances (chisel.instances/get-instance-list docs)
        tm (chisel.lda/run-lda instances :T 10 :numiter 50 :topwords 50 :alpha 0.5)]
    (chisel.lda/write-topics tm "example.topics" :topwords 50)
    ))


(defn -main []
  (println (str "We will make a topic model."))
  (tmx)
)

    