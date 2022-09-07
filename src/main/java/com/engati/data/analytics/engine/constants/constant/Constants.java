package com.engati.data.analytics.engine.constants.constant;

import java.text.SimpleDateFormat;

public class Constants {

  public static final String RESPONSE_OBJECT = "response";
  public static final String CSV_BASE_PATH_FORMAT = "/opt/engati/customer_segments/%s";
  public static final String CSV_PATH_FORMAT = "/opt/engati/customer_segments/%s/%s.csv";
  public static final String PDE_PREFIX = "pde";
  public static final String RETROFIT_PDE_API = "retrofitPDEApi";
  public static final String PDE_CLIENT = "pdeHttpClient";
  public static final String RETROFIT_PDE_SERVICE = "retrofitPDERestService";
  public static final String PDE_EXECUTE_QUERY = "/v1/discovery/query/dynamic/bot/{botRef}/domain/{domain}";
  public static final String BOTREF = "botRef";
  public static final String DOMAIN = "domain";
  public static final String RESPONSE = "responseObject";
  public static final String ABANDONED_CHECKOUTS = "abandoned_checkouts";


  //  DuckDB Connection URL
  public static String DUCKDB_CONNECTION_URI = "jdbc:duckdb:";
  public static String PARQUET_FILE_PATH = "/opt/engati/parquet_store";

//  Defaults
  public static Long DEFAULT_BOTREF = -1L;
  public static Integer DEFAULT_ORDER_VALUE = 0;
  public static Long DEFAULT_AOV_VALUE = 0L;
  public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  //  Generic Constants
  public static String BOT_REF = "botRef";
  public static String CUSTOMER_ID = "customer_id";
  public static String CUSTOMER_EMAIL = "customer_email";
  public static String CUSTOMER_NAME = "customer_name";
  public static String CUSTOMER_PHONE = "customer_phone";
  public static String VARIANT_ID = "variant_id";
  public static String PRODUCT_ID = "product_id";
  public static String DATE_FORMAT = "yyyy-MM-dd";
  public static String CREATED_DATE = "created_date";
  public static String QUERIES_ASKED = "queries_asked";
  public static String QUERIES_UNANSWERED= "queries_unanswered";


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


  public static final String PRODUCT_DETAILS_REQUEST = "{\n" + "    \"entities\": [ \n"
      + "        \"PRODUCT\",\n" + "        \"IMAGE\"\n" + "    ],\n" + "    \"searchFilters\": [\n"
      + "        {\n" + "            \"isOrCondition\": false,\n"
      + "            \"fieldName\": \"PRODUCT_productId\",\n" + "            \"value\": null,\n"
      + "            \"values\": %s,\n"
      + "            \"condition\": \"IN\"\n" + "        }\n" + "    ],\n" + "    \"fields\": [\n"
      + "        \"PRODUCT_productId\",\n" + "        \"PRODUCT_title\",\n"
      + "        \"IMAGE_url\"\n" + "    ],\n" + "    \"customSort\": [\n" + "        {\n"
      + "            \"name\": \"PRODUCT_productId\",\n" + "            \"isAsc\": true\n"
      + "        }\n" + "    ],\n" + "    \"pageNumber\": 1,\n" + "    \"pageSize\": 10\n" + "}";

  public static final String[] INTENT_LIST = new String[]{"ORDER_ENQUIRY", "PURCHASE_ENQUIRY", "RETURN_AND_EXCHANGE",
                                                          "PRICE_ENQUIRY", "OUT_OF_STOCK_ENQUIRY"};
  public static final String[] SENTIMENT_LIST = new String[]{"Positive", "Negative", "Neutral"};

}
