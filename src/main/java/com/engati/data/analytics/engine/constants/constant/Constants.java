package com.engati.data.analytics.engine.constants.constant;

public class Constants {
//  DuckDB Connection URL
  public static String DUCKDB_CONNECTION_URI = "jdbc:duckdb:";
//  public static String PAQUET_FILE_PATH = "/opt/engati/shopify-etl-engine/parquet_store";
  public static String PARQUET_FILE_PATH = "/opt/engati/parquet_store";
// public static String PARQUET_FILE_PATH = "/Users/jaymehta/Desktop/PycharmProjects/shopify-etl-engine/parquet_store";

//  Defaults
  public static Long DEFAULT_BOTREF = -1L;
  public static Long DEFAULT_CUSTOMER_ID = -1L;
  public static Long DEFAULT_ORDER_VALUE = 0L;
  public static Long DEFAULT_AOV_VALUE = 0L;


  //  Generic Constants
  public static String BOT_REF = "botRef";
  public static String CUSTOMER_ID = "customer_id";
  public static String CUSTOMER_EMAIL = "customer_email";
  public static String CUSTOMER_NAME = "customer_name";
  public static String CUSTOMER_PHONE = "customer_phone";
  public static String VARIANT_ID = "variant_id";
  public static String PRODUCT_ID = "product_id";


}
