package com.engati.data.analytics.engine.util;

public class Constants {
  public static final String DRUID_RESPONSE_API_PATH =
      "/customer/{customerId}/bot/{botRef}/druid/response";
  public static final String DRUID_INGESTION_API_PATH =
      "/customer/{customerId}/bot/{botRef}/ingest";
  public static final String DRUID_SQL_RESPONSE_API_PATH =
      "/customer/{customerId}/bot/{botRef}/sql/response";
  public static final String DRUID_TASK_RESPONSE_API_PATH =
      "/ingestion/tasks";
  public static final String REQ_PARAM_CUSTOMER_ID = "customerId";
  public static final String REQ_PARAM_BOT_REF = "botRef";
  public static final String REQ_PARAM_IS_INITIAL = "isInitialLoad";
  public static final String REQ_PARAM_DATA_SOURCE_NAME = "dataSourceName";
  public static final String REQ_PARAM_TIMESTAMP = "timestamp";

  public static final String TOP = "top";;
  public static final String TIMESTAMP = "timestamp";
  public static final String RESULT = "result";
  public static final String EVENT = "event";

  public static final String GROWTH_METRIC = "growth_metric";
  public static final String INITIATED_SALES = "initiated_sales";
  public static final String CART_ABANDONMENT = "cart_abandonment";
  public static final String ISO_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'";
  public static final String YEAR = "YEAR";
  public static final String MONTH = "MONTH";
  public static final String DAY = "DAY";
  public static final String QUARTER = "QUARTER";
  public static final String WEEK = "WEEK";
  public static final String NOT_APPLICABLE = "N/A";

}

