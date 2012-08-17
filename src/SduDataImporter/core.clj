(ns SduDataImporter.core
  (:require clojure.java.io)
  (:require clojure-solr)
  (:require clojure.string)
  (:import [java.io File]))

;; GLOBALS

(def columnSeparator #"\|")

(def firstColName "Activity Row Id")

(def columnNames ["activity row id" "created date" "primary contact id" "contact integration id" "case" "member" "parish data" "start time" "call type" "type" "inin id" "parish" "interaction time" "resolution" "notes" "division" "program" "task status" "contact external unique id" "subject" "description" "contact type" "caller id" "crmit inin id" "validation status" "worker office number"])

(def dirToProcess "/Users/julien/Documents/Output/")

(def patFilesToProcess #"LAActivity_AIMA_Part_(.*)\.txt")

;; UTILS

(def connection (clojure-solr/connect "http://127.0.0.1:8983/solr"))

(def patDateTime #"(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2}:\d{2})")


;; FUNCTIONS

(defn changeDateFormat [date]
  "Change date format from '23-11-2012 03:34:51' to solr format '2012-11-23T03:34:51Z'"
  (let [match (re-matches patDateTime date)]
    (if match
      (str (nth match 3) "-" (nth match 2) "-" (nth match 1) "T" (nth match 4) "Z"))))

(defn lineToMap [line]
  "Transforms the sequence of fields into a map, and change date fields format"
  (let [mapLine (zipmap columnNames (clojure.string/split line columnSeparator))
        keys ["created date" "start time"]]
    ;(println (str mapLine))
    (reduce (fn [m k] (assoc m k (changeDateFormat (m k)))) mapLine keys)
    ))
     

(defn indexLine [line]
  "Index a line"
  (let [mapLine (lineToMap line)]
    (if-not (= 0 (compare firstColName (mapLine "activity row id")))
      (clojure-solr/with-connection connection
        (clojure-solr/add-document! mapLine)))))


(defn parseFile [fileName]
  "Parse a file"
  (println (str "Reading file " (.getName fileName)))
  (with-open [rdr (clojure.java.io/reader fileName)]
    (doseq [line (line-seq rdr)]
      ;(println (str line))
      ;(println (str (clojure.string/split line columnSeparator)))
      (indexLine line)))
  (clojure-solr/with-connection connection
    (clojure-solr/commit!)))

(defn parseDir [dirName]
  "Parse a directory"
  (println (str "Reading directory " dirName))
  (doseq [f (.listFiles (File. dirName))]
    (if (re-matches patFilesToProcess (.getName f))
      (parseFile f))))

(defn -main
  []
  (println (str "We will read directory " dirToProcess))
  (parseDir dirToProcess))


    