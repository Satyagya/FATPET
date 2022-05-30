package com.engati.data.analytics.engine.constants.constant;

import java.util.Locale;

public class Constants {

  public static final String RESPONSE_OBJECT = "response";
  public static final String CSV_BASE_PATH_FORMAT = "/opt/engati/customer_segments/%s";
  public static final String CSV_PATH_FORMAT = "/opt/engati/customer_segments/%s/%s.csv";
//  public static final String CSV_BASE_PATH_FORMAT = "/Users/jaymehta/Desktop/IdeaProjects/data-analytics-engine/src/main/resources/customer_segments/%s";
//  public static final String CSV_PATH_FORMAT = "/Users/jaymehta/Desktop/IdeaProjects/data-analytics-engine/src/main/resources/customer_segments/%s/%s.csv";



  //  DuckDB Connection URL
  public static String DUCKDB_CONNECTION_URI = "jdbc:duckdb:";
  public static String PARQUET_FILE_PATH = "/opt/engati/parquet_store";
//  public static String PARQUET_FILE_PATH = "/Users/jaymehta/Desktop/PycharmProjects/shopify-etl-engine/parquet_store";

//  Defaults
  public static Long DEFAULT_BOTREF = -1L;
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
  public static final String DUCKDB_ENGINE_PREFIX = "duckdb.engine";
  public static final String DUCK_DB_EXECUTE_QUERY = "/v1.0/api/duckdb/query/";
  public static final String DUCK_DB_EXECUTE_QUERY_DETAILS = "/v1.0/api/duckdb/query/details/";
  public static final String RETROFIT_DUCKDB_ENGINE_API = "retrofitDuckdbEngineApi";
  public static final String DUCKDB_ENGINE_HTTP_CLIENT = "DuckdbEngineHttpClient";
  public static final String RETROFIT_DUCKDB_ENGINE_REST_SERVICE = "retrofitDuckdbEngineRestService";

  public static final String QUERY = "query";
  public static final String KEY = "key";

  public static final String[] CUSTOMER_SEGMENT_HEADER = new String[]{"CUSTOMER NAME", "CUSTOMER EMAIL", "CUSTOMER PHONE",
          "STORE AOV", "CUSTOMER AOV", "ORDERS IN LAST ONE MONTH", "ORDERS IN LAST SIX MONTH", "ORDERS IN LAST TWELVE MONTH"};



}
