(ns SduDataImporter.core
  (:require clojure.java.io)
  (:require clojure-solr)
  (:require clojure.string))

(def columnSeparator #"\|")

(def columnNames ["activity row id" "created date" "primary contact id" "contact integration id" "case" "member" "parish data" "start time" "call type" "type" "inin id" "parish" "interaction time" "resolution" "notes" "division" "program" "task status" "contact external unique id" "subject" "description" "contact type" "caller id" "crmit inin id" "validation status" "worker office number"])

(defn lineToMap [line]
  (zipmap columnNames (clojure.string/split line columnSeparator)))
  

(defn indexLine [line]
  (println (str (lineToMap line)))
  )

(defn parseFile [fileName]
  (with-open [rdr (clojure.java.io/reader fileName)]
    (doseq [line (line-seq rdr)]
      (indexLine line))))


(defn -main
  [fileName]
  (println (str "We will read " fileName))
  (parseFile fileName))


    