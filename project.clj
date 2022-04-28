(defproject metabase/teradata-driver "1.0.2-metabase-v0.40.8-teradata-jdbc-17.10"
  :min-lein-version "2.5.0"

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :omit-source   true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "teradata.metabase-driver.jar"}})
