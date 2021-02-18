package com.engati.data.analytics.engine.util;

import java.text.DecimalFormat;

public class Constants {
  public static final String DRUID_RESPONSE_API_PATH =
      "/customer/{customerId}/bot/{botRef}/druid/response";
  public static final String DRUID_INGESTION_API_PATH =
      "/customer/{customerId}/bot/{botRef}/ingest";
  public static final String DRUID_SQL_RESPONSE_API_PATH =
      "/customer/{customerId}/bot/{botRef}/sql/response";
  public static final String REQ_PARAM_CUSTOMER_ID = "customerId";
  public static final String REQ_PARAM_BOT_REF = "botRef";
  public static final String REQ_PARAM_IS_INITIAL = "isInitialLoad";
  public static final String REQ_PARAM_DATA_SOURCE_NAME = "dataSourceName";
  public static final String REQ_PARAM_TIMESTAMP = "timestamp";

  public static final String TOP = "top";
  public static final String ORDER_COUNT = "order_count";
  public static final String SESSION_COUNT = "session_count";
  public static final String BILLING_CITY = "billing_city";
  public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");
  public static final String RESULT = "result";
  public static final String EVENT = "event";
}
