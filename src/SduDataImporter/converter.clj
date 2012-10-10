(ns SduDataImporter.converter
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

(def dirOutput  "/Users/julien/Documents/AfterCall/Output/csv/")

;; UTILS

 (def patDateTime #"(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2}:\d{2})")


;; FUNCTIONS


(defn parseFile [fileName]
  "Parse a file, check the nb of columns, and output the csv."
  (println (str "Reading file " (.getName fileName)))
  (let [name (.getName fileName)
        outFile (str dirOutput (subs name 0 (- (count name) 4)) ".csv")]

    (with-open [writer (clojure.java.io/writer outFile)]
      (with-open [rdr (clojure.java.io/reader fileName)]
        (doseq [line (line-seq rdr)]

          ; -1 allows to keep all trailing empty columns
          (let [splitted (clojure.string/split line COLUMN_SEPARATOR -1)
                ];_ (println (count splitted) (count COLUMN_NAMES))]

            (if (= (count splitted) (count COLUMN_NAMES))
              (.write writer (str (apply str (interpose "|" splitted)) "\n"))  
              (println (str "Bad line" splitted "\n" line)))))))))


(defn parseDir [dirName]
  "Parse a directory"
  (println (str "Reading directory " dirName))
  (doseq [f (.listFiles (File. dirName))]
    (if (re-matches patFilesToProcess (.getName f))
      (parseFile f))))

(defn -main []
  (println (str "We will read directory " dirToProcess " and output to " dirOutput))
  (parseDir dirToProcess))


    