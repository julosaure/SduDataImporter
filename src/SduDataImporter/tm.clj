(ns SduDataImporter.tm
  (:require clojure.java.io)
  (:require [clojure-solr :as cs])
  (:require clojure.string)
  (:use (chisel instances lda sample))
  (:import [java.io File]))

;; GLOBALS




;; UTILS

(def connection (clojure-solr/connect "http://127.0.0.1:8983/solr"))

(def PAT_REASON #"(REASON|Reason|PURPOSE)(( FOR| For)?( THE| The)?( )?(CALL|Call))?[-:]")

;; FUNCTIONS

(defn cleanDescription [desc]
  (let [match (re-matcher PAT_REASON desc)]
    (clojure.string/lower-case 
     (if (.matches match)
       ((println (str match desc))
       (subs desc (.end match)))
       desc
       ))))


(defn rowToDoc [row]
  [(row "activity_row_id") (cleanDescription (row "description"))])
  ;[(row "activity_row_id") (clojure.string/lower-case (row "description"))])

(defn querySolr []
  (println "Query Solr")
  (cs/with-connection connection
    (cs/search "*:*" "rows" 100000 "fl" "description,activity_row_id")))


(defn tmx []
  (let [doc1 (map rowToDoc (querySolr))
        docs (apply hash-map (flatten doc1))
        instances (chisel.instances/get-instance-list docs)
        tm (chisel.lda/run-lda instances :T 50 :numiter 50 :topwords 15 :alpha 0.5)]
    (chisel.lda/write-topics tm "example.topics" :topwords 50)
    ;;(chisel.sample/get-sample tm)
    ;;(chisel.lda/write-sample tm "sample.txt")
    ;;(chisel.lda/write-document-topic tm "document-topic.txt")
    ;;(.printState tm (new File "state.txt"))
    ))


(defn -main []
  (println "We will make a topic model.")
  (tmx)
)

    