(ns SduDataImporter.importer
  (:require clojure.java.io)
  (:require clojure-solr)
  (:require clojure.string)
  (:import [java.io File]))

;; GLOBALS

(def COLUMN_SEPARATOR #"\|")

(def FIRST_COL_NAME "Activity Row Id")

(def COLUMN_NAMES ["activity_row_id" "created_date" "primary_contact_id" "contact_integration_id" "case" "member" "parish_data" "start_time" "call_type" "type" "inin_id" "parish" "interaction_time" "resolution" "notes" "division" "program" "task_status" "contact_external_unique_id" "subject" "description" "contact_type" "caller_id" "crmit_inin_id" "validation_status" "worker_office_number"])

(def DATE_FIELDS ["created_date" "start_time"])

(def dirToProcess "/Users/julien/Documents/AfterCall/Output/")

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
  (let [mapLine (zipmap COLUMN_NAMES (clojure.string/split line COLUMN_SEPARATOR))
        keys DATE_FIELDS]
    ;(println (str mapLine))
    (reduce (fn [m k] (assoc m k (changeDateFormat (m k)))) mapLine keys)
    ))
     

(defn indexLine [line]
  "Index a line"
  (let [mapLine (lineToMap line)]
    (if-not (= 0 (compare FIRST_COL_NAME (mapLine "activity_row_id")))
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


    