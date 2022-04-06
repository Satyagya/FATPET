package com.engati.data.analytics.engine.constants.constant;

public class Constants {

  public static final String RESPONSE_OBJECT = "response";
  public static final String RESPONSE_OBJECT_FOR_EXECUTE_QUERY = "$.response.customer_id";
  //  DuckDB Connection URL
  public static String DUCKDB_CONNECTION_URI = "jdbc:duckdb:";
//  public static String PAQUET_FILE_PATH = "/opt/engati/shopify-etl-engine/parquet_store";
//  public static String PARQUET_FILE_PATH = "/opt/engati/parquet_store";
 public static String PARQUET_FILE_PATH = "/Users/swastikn/engati-projects/DAE/data-analytics-engine/parquet_store";

//  Defaults
  public static Long DEFAULT_BOTREF = -1L;
  public static Long DEFAULT_CUSTOMER_ID = -1L;
  public static Integer DEFAULT_ORDER_VALUE = 0;
  public static Long DEFAULT_AOV_VALUE = 0L;

  //  Generic Constants
  public static String BOT_REF = "botRef";
  public static String CUSTOMER_ID = "customer_id";
  public static String CUSTOMER_EMAIL = "customer_email";
  public static String CUSTOMER_NAME = "customer_name";
  public static String CUSTOMER_PHONE = "customer_phone";
  public static String VARIANT_ID = "variant_id";
  public static String PRODUCT_ID = "product_id";

  // Retrofit
  public static final String ETL_ENGINE_PREFIX = "etl.engine";
  public static final String DUCK_DB_EXECUTE_QUERY = "/v1.0/api/duckdb/query/";
  public static final String DUCK_DB_EXECUTE_QUERY_DETAILS = "/v1.0/api/duckdb/query/details/";
  public static final String RETROFIT_ETL_ENGINE_API = "retrofitEtlEngineApi";
  public static final String ETL_ENGINE_HTTP_CLIENT = "EtlEngineHttpClient";
  public static final String RETROFIT_ETL_ENGINE_REST_SERVICE = "retrofitEtlEngineRestService";

  public static final String QUERY = "query";
  public static final String KEY = "key";





}
