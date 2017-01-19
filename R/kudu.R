# Kudu Sparklyr Extension
# Intended to provide access to Kudu tables in Sparklyr
#
#

#' @import sparklyr
#' @import magrittr

#' @export
kudu_context <- function(sc,kudu_master) {
  kc <- invoke_new(sc,"org.apache.kudu.spark.kudu.KuduContext",kudu_master)
  new_sc <- sc
  new_sc$kudu_context = kc
  new_sc$kudu_master = kudu_master
  new_sc
}

#' @export
read_kudu_table <- function(sc,kudu_table) {
  hc <- sc$hive_context
  kudu_master <- sc$kudu_master
  options <- list("kudu.master"=kudu_master,"kudu.table"=kudu_table)
  df <- hc %>%
    invoke("read") %>%
      invoke("format","org.apache.kudu.spark.kudu") %>%
        invoke("option","kudu.master",kudu_master) %>%
          invoke("option","kudu.table",kudu_table) %>%
            invoke("load")

  df
}

#' @export
kudu_table_exists <- function(sc,kudu_table){
  get_kudu_context(sc) %>% invoke("tableExists",kudu_table)
}
#' @export
create_kudu_table <- function(sc,table_name,schema,keys,options){
  get_kudu_context(sc) %>% invoke("createTable",table_name,schema,keys,options) 
    
}

#' create_kudu_impala_table
#'
#' @param sc - spark connection
#' @param database - name of the impala/hive database where table will be created
#' @param table_name - table name
#' @param schema - table colum definitions
#' @param keys - list of key colums
#' @param options - kudu table options
#'
#' @return 
#' @export
#'
#' @examples
#' 
create_kudu_impala_table <- function(sc, database, table_name, schema, keys, options){
  # create table in impala
  paste0("CREATE EXTERNAL TABLE `", database, ".", table_name, "` (
    `uwi` STRING,
    `uwi_nexen` STRING,
    `well_name` STRING,
    `license_num` STRING,
    `drain_area` STRING,
    `project` STRING,
    `lease` STRING,
    `surface_pad` STRING,
    `well_pair_id` STRING,
    `well_type` STRING,
    `well_subtype` STRING,
    `proj_phase` STRING,
    `lease_operator` STRING,
    `latitude` FLOAT,
    `longitude` FLOAT,
    `bottom_hole_latitude` FLOAT,
    `bottom_hole_longitude` FLOAT,
    `kb_elev` FLOAT,
    `max_tvd` FLOAT,
    `drill_td` FLOAT,
    `abandonment_date` STRING,
    `completion_date` STRING,
    `confidential_date` STRING,
    `effective_date` STRING,
    `expiry_date` STRING,
    `final_drill_date` STRING,
    `rig_on_site_date` STRING,
    `rig_release_date` STRING,
    `spud_date` STRING
  )
  TBLPROPERTIES(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
    'kudu.table_name' = ' kudu_test_1 ',
    'kudu.master_addresses' = 'calxhad1wrk1p.global.ad:7051,calxhad1wrk3p.global.ad:7051,calxhad1wrk5p.global.ad:7051,calxhad1wrk7p.global.ad:7051',
    'kudu.key_columns' = 'uwi'
  );")
}


#' @export
delete_kudu_table <- function(sc,kudu_table){
  resp <- sc$kudu_context %>% invoke("deleteTable",kudu_table)
  exists("resp")
}
#' @export
kudu_insert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("insertRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_insert_ignore_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("insertIgnoreRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_upsert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("upsertRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_update_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("updateRows",spark_dataframe(df),kudu_table)
  exists("resp")  
}
#'@export
kudu_delete_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("deleteRows",spark_dataframe(df),kudu_table)
  exists("resp")  
}
#'@export
sdf_schema <- function(df){
  spark_dataframe(df) %>% invoke("schema")
}

get_kudu_context <- function(sc){
  sc$kudu_context
}
add_sql_context <- function(sc){
  jsc <- sc$java_context
  sql_context <- invoke_static(
    sc,
    "org.apache.spark.sql.api.r.SQLUtils",
    "createSQLContext",
    jsc
  )
  sc$hive_context = sql_context
  sc
}

