(ns metabase.driver.teradata
  (:require [clojure
             [set :as set]
             [string :as s]]
            [clojure.java.jdbc :as jdbc]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.deduplicate :as deduplicateutil]
            [metabase.models.field :as field]
            [metabase.query-processor.util :as qputil]
            [metabase.util
             [honeysql-extensions :as hx]
             [ssh :as ssh]])
  (:import [java.sql DatabaseMetaData ResultSet]))

(driver/register! :teradata, :parent :sql-jdbc)

(defmethod sql-jdbc.sync/database-type->base-type :teradata [_ column-type]
  ({:BIGINT        :type/BigInteger
    :BIGSERIAL     :type/BigInteger
    :BIT           :type/*
    :BLOB          :type/*
    :BOX           :type/*
    :CHAR          :type/Text
    :CLOB          :type/Text
    :BYTE          :type/*
    :BYTEINT       :type/Integer
    :DATE          :type/Date
    :DECIMAL       :type/Decimal
    :FLOAT         :type/Float
    :FLOAT4        :type/Float
    :FLOAT8        :type/Float
    :INTEGER       :type/Integer
    :INT           :type/Integer
    :INT2          :type/Integer
    :INT4          :type/Integer
    :INT8          :type/BigInteger
    :INTERVAL      :type/* ; time span
    :JSON          :type/Text
    :LONGVARCHAR   :type/Text ; Teradata extension
    :LSEG          :type/*
    :MACADDR       :type/Text
    :MONEY         :type/Decimal
    :NUMERIC       :type/Decimal
    :PATH          :type/*
    :POINT         :type/*
    :REAL          :type/Float
    :SERIAL        :type/Integer
    :SERIAL2       :type/Integer
    :SERIAL4       :type/Integer
    :SERIAL8       :type/BigInteger
    :SMALLINT      :type/Integer
    :SMALLSERIAL   :type/Integer
    :TIME          :type/Time
    :TIMETZ        :type/Time
    :TIMESTAMP     :type/DateTime
    :TIMESTAMPTZ   :type/DateTime
    (keyword "TIMESTAMP WITH TIME ZONE") :type/DateTime
    :TSQUERY       :type/*
    :TSVECTOR      :type/*
    :TXID_SNAPSHOT :type/*
    :UUID          :type/UUID
    :VARBIT        :type/*
    :VARBYTE       :type/* ; byte array
    :VARCHAR       :type/Text
    :XML           :type/Text
    (keyword "bit varying")                :type/*
    (keyword "character varying")          :type/Text
    (keyword "double precision")           :type/Float
    (keyword "time with time zone")        :type/Time
    (keyword "time without time zone")     :type/Time
    (keyword "timestamp with timezone")    :type/DateTime
    (keyword "timestamp without timezone") :type/DateTime}, column-type))

(defn- dbnames-set
  "Transform the string of databases to a set of strings."
  [dbnames]
  (when dbnames
    (set (map #(s/trim %) (s/split (s/trim dbnames) #",")))))

(defn- teradata-spec
  "Create a database specification for a teradata database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port dbnames charset tmode encrypt-data]
    :or   {host "localhost", charset "UTF8", tmode "ANSI", encrypt-data true}
    :as   opts}]
  (merge {:classname   "com.teradata.jdbc.TeraDriver"
          :subprotocol "teradata"
          :subname     (str "//" host "/"
                            (->> (merge
                                   (when dbnames
                                     {"DATABASE" (first (dbnames-set dbnames))})
                                   (when port
                                     {"DBS_PORT" port})
                                   {"CHARSET"             charset
                                    "TMODE"               tmode
                                    "ENCRYPTDATA"         (if encrypt-data "ON" "OFF")
                                    "FINALIZE_AUTO_CLOSE" "ON"
                                    ;; We don't need lob support in metabase. This also removes the limitation of 16 open statements per session which would interfere metadata crawling.
                                    "LOB_SUPPORT"         "OFF"
                                    })
                              (map #(format "%s=%s" (first %) (second %)))
                              (clojure.string/join ",")))
          :delimiters  "`"}
         (dissoc opts :host :port :dbnames :tmode :charset)))

(defmethod sql-jdbc.conn/connection-details->spec :teradata
  [_ {ssl? :ssl, :as details-map}]
  (-> details-map
    teradata-spec
    (sql-jdbc.common/handle-additional-options details-map, :seperator-style :comma)))

;; trunc always returns a date in Teradata
(defn- date-trunc [unit expr] (hsql/call :trunc expr (hx/literal unit)))

(defn- timestamp-trunc [unit expr] (hsql/call :to_timestamp
                                              (hsql/call :to_char 
                                                         expr
                                                         unit) unit))

(defn- extract    [unit expr] (hsql/call :extract unit expr))

(def ^:private extract-integer (comp hx/->integer extract))

(def ^:private ^:const one-day (hsql/raw "INTERVAL '1' DAY"))

(def ^:private ^:const now (hsql/raw "CURRENT_TIMESTAMP"))

(defmethod sql.qp/date [:teradata :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:teradata :minute]          [_ _ expr] (timestamp-trunc "yyyy-mm-dd hh24:mi" expr))
(defmethod sql.qp/date [:teradata :minute-of-hour]  [_ _ expr] (extract-integer :minute expr))
(defmethod sql.qp/date [:teradata :hour]            [_ _ expr] (timestamp-trunc "yyyy-mm-dd hh24" expr))
(defmethod sql.qp/date [:teradata :hour-of-day]     [_ _ expr] (extract-integer :hour expr))
(defmethod sql.qp/date [:teradata :day]             [_ _ expr] (hx/->date expr))
(defmethod sql.qp/date [:teradata :day-of-week]     [_ _ expr] (hx/inc (hx/- (sql.qp/date :day expr)
                                                                             (sql.qp/date :week expr))))
(defmethod sql.qp/date [:teradata :day-of-month]    [_ _ expr] (extract-integer :day expr))
(defmethod sql.qp/date [:teradata :day-of-year]     [_ _ expr] (hx/inc (hx/- (sql.qp/date :day expr) (date-trunc :year expr))))
(defmethod sql.qp/date [:teradata :week]            [_ _ expr] (date-trunc :day expr)) ; Same behaviour as with Oracle.
(defmethod sql.qp/date [:teradata :week-of-year]    [_ _ expr] (hx/inc (hx// (hx/- (date-trunc :iw expr)
                                                                                   (date-trunc :iy expr))
                                                                             7)))
(defmethod sql.qp/date [:teradata :month]           [_ _ expr] (date-trunc :mm expr))
(defmethod sql.qp/date [:teradata :month-of-year]   [_ _ expr] (extract-integer :month expr))
(defmethod sql.qp/date [:teradata :quarter]         [_ _ expr] (date-trunc :q expr))
(defmethod sql.qp/date [:teradata :quarter-of-year] [_ _ expr] (hx// (hx/+ (sql.qp/date :month-of-year (sql.qp/date :quarter expr)) 2) 3))
(defmethod sql.qp/date [:teradata :year]            [_ _ expr] (extract-integer :year expr))

(defmethod sql.qp/unix-timestamp->timestamp [:teradata :seconds] [_ _ field-or-value]
  (hsql/call :to_timestamp field-or-value))

(defmethod sql.qp/unix-timestamp->timestamp [:teradata :milliseconds] [_ _ field-or-value]
  (sql.qp/unix-timestamp->timestamp (hx// field-or-value 1000) :seconds))

(defmethod sql.qp/apply-top-level-clause [:teradata :limit] [_ _ honeysql-form {value :limit}]
  (update (assoc honeysql-form :modifiers [(format "TOP %d" value)]) :select deduplicateutil/deduplicate-identifiers))

(defmethod sql.qp/apply-top-level-clause [:teradata :page] [_ _ honeysql-form {{:keys [items page]} :page}]
  (assoc honeysql-form :offset (hsql/raw (format "QUALIFY ROW_NUMBER() OVER (%s) BETWEEN %d AND %d"
                                                 (first (hsql/format (select-keys honeysql-form [:order-by])
                                                                     :allow-dashed-names? true
                                                                     :quoting :ansi))
                                                 (inc (* items (dec page)))
                                                 (* items page)))))

(def excluded-schemas
  #{"SystemFe" "SYSLIB" "LockLogShredder" "Sys_Calendar" "SYSBAR" "SYSUIF"
    "dbcmngr" "tdwm" "TDStats" "TDQCD" "SQLJ" "SysAdmin" "SYSSPATIAL" "DBC" "Crashdumps" "External_AP" "TDPUSER"})

(defmethod sql-jdbc.sync/excluded-schemas :teradata [_]
  excluded-schemas)

;; Teradata uses ByteInt with values `1`/`0` for boolean `TRUE`/`FALSE`.
(defmethod sql.qp/->honeysql [:teradata Boolean]
  [_ bool]
  (if bool 1 0))

(defn- get-tables
  "Fetch a JDBC Metadata ResultSet of tables in the DB, optionally limited to ones belonging to a given schema."
  ^ResultSet [^DatabaseMetaData metadata, ^String schema-or-nil]
  (jdbc/result-set-seq (.getTables metadata nil schema-or-nil "%" ; tablePattern "%" = match all tables
                         (into-array String ["TABLE", "VIEW", "FOREIGN TABLE"]))))

(defn- fast-active-tables
  "Teradata, fast implementation of `fast-active-tables` to support inclusion list."
  [driver, ^DatabaseMetaData metadata, {{:keys [dbnames]} :details, :as database}]
  (let [all-schemas (set (map :table_schem (jdbc/result-set-seq (.getSchemas metadata))))
        dbs (dbnames-set dbnames)
        schemas     (if (empty? dbs)
                      (set/difference all-schemas excluded-schemas) ; use default exclusion list
                      (set/intersection all-schemas dbs))] ; use defined inclusion list
    (set (for [schema schemas
               table-name (mapv :table_name (get-tables metadata schema))]
           {:name   table-name
            :schema schema}))))

;; Overridden to have access to the database with the configured property dbnames (inclusion list)
;; which will be used to filter the schemas.
(defmethod driver/describe-database :teradata [driver database]
  (jdbc/with-db-metadata [metadata (sql-jdbc.conn/db->pooled-connection-spec database)]
    {:tables (fast-active-tables, driver, ^DatabaseMetaData metadata, database)}))

(defn- run-query
  "Run the query itself without setting the timezone connection parameter as this must not be changed on a Teradata connection.
   Setting connection attributes like timezone would make subsequent queries behave unexpectedly."
  [{sql :query, params :params, remark :remark} timezone connection]
  (let [sql              (s/replace (s/replace (str "-- " remark "\n" sql) "OFFSET" "") "test_data" "test-data") ;; temporary hack
        statement        (into [sql] params)
        [columns & rows] (jdbc/query connection statement {:identifiers    identity, :as-arrays? true
                                                           :read-columns   (#'metabase.driver.sql-jdbc.execute/read-columns :teradata timezone)})]
    {:rows    (or rows [])
     :columns columns}))

(defn- run-query-without-timezone [driver settings connection query]
  (#'metabase.driver.sql-jdbc.execute/do-in-transaction connection (partial run-query query nil)))

(defmethod driver/execute-query :teradata
  [driver {:keys [database settings], query :native, :as outer-query}]
  (let [query (assoc query :remark (qputil/query->remark outer-query))]
    (#'metabase.driver.sql-jdbc.execute/do-with-try-catch
      (fn []
        (let [db-connection (sql-jdbc.conn/db->pooled-connection-spec database)]
          (run-query-without-timezone driver settings db-connection query))))))

(defmethod sql.qp/current-datetime-fn :teradata [_] now)

; TODO check if overriding apply-top-level-clause could make nested queries work
(defmethod driver/supports? [:teradata :nested-queries] [_ _] false)

(defmethod driver/date-add :teradata [_ dt amount unit]
  (if (>= amount 0)
    (hx/+ (hx/->timestamp dt) (hsql/raw (format "INTERVAL '%d' %s" (int amount) (name unit))))
    (hx/- (hx/->timestamp dt) (hsql/raw (format "INTERVAL '%d' %s" (Math/abs (int amount)) (name unit))))))
