(defproject SduDataImporter "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"],
                 [clojure-solr "0.4.0-SNAPSHOT"],
                 ;;[icarus "0.1a"],
                 [chisel "1.0.0-SNAPSHOT"]]
  :jvm-opts ["-Xmx515m"]
  :main SduDataImporter.converter
  )