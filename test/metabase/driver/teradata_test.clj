(ns metabase.driver.teradata-test
  (:require [expectations :refer :all]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]))

;; Check that additional JDBC options are handled correctly. This is comma separated for Teradata.
(expect
  {:classname                   "com.teradata.jdbc.TeraDriver"
   :subprotocol                 "teradata"
   :subname                     "//localhost/CHARSET=UTF8,TMODE=ANSI,ENCRYPTDATA=ON,FINALIZE_AUTO_CLOSE=ON,LOB_SUPPORT=OFF,COP=OFF"
   :delimiters                  "`"}
  (-> (sql-jdbc.conn/connection-details->spec :teradata {:host "localhost"
                                                         :additional-options  "COP=OFF"})))