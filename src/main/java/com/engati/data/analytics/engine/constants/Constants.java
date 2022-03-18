package com.engati.data.analytics.engine.constants;

public class Constants {
//  DUckDB Connectrion URL
  public static String DUCKDB_CONNECTION_URI = "jdbc:duckdb:";

//  Defaults
  public static Long DEFAULT_BOTREF = ((long) -1);
  public static Long DEFAULT_CUSTOMER_ID = ((long) -1);
  public static Long DEFAULT_ORDER_VALUE = Long.valueOf(0);
  public static Long DEFAULT_AOV_VALUE = Long.valueOf(0);

//  Operators
  public static String LTE = "LTE";
  public static String LTE_OPERATOR = "<=";
  public static String LT = "LT";
  public static String LT_OPERATOR = "<";
  public static String GT = "GT";
  public static String GT_OPERATOR = ">";
  public static String GTE = "GTE";
  public static String GTE_OPERATOR = ">=";
  public static String EQUAL_OPERATOR = "=";

  //  Query Variations and Replacements
  public static String LAST_ORDER_DATE = "LAST_ORDER_DATE";
  public static String MAX = "max";
  public static String MIN = "min";
  public static String CLOSING_SQUARE_BRACKET = "]";
  public static String OPENING_SQUARE_BRACKET = "[";
  public static String CLOSING_ROUND_BRACKET = ")";
  public static String OPENING_ROUND_BRACKET = "(";
  public static String MONTHS = "months";
  public static String MONTH = "month";
  public static String CUSTOMER_SET = "customerSet";
  public static String GAP = "gap";
  public static String INTERVAL = "interval";
  public static String COLUMN_NAME= "col_name";
  public static String AGGREGATOR = "aggregator";
  public static String OPERATOR = "operator";
  public static String ORDERS_CONFIGURED = "orders_configured";
  public static String VALUE = "value";
  public static String METRIC = "metric";
  public static String STORE_AOV = "STORE_AOV";
  public static String AOV = "AOV";
  public static String ORDERS_LAST_12_MONTHS = "orders__last_12_months";
  public static String ORDERS_LAST_6_MONTHS = "orders__last_6_months";
  public static String ORDERS_LAST_1_MONTH = "orders__last_1_month";


  //  Generic Constants
  public static String BOT_REF = "botRef";
  public static String CUSTOMER_ID = "customer_id";
  public static String CUSTOMER_EMAIL = "customer_email";
  public static String CUSTOMER_NAME = "customer_name";
  public static String CUSTOMER_PHONE = "customer_phone";
  public static Integer ONE = 1;
  public static Integer SIX = 6;
  public static Integer TWELVE = 12;


}
