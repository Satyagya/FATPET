package com.engati.data.analytics.engine.util;

import java.text.DecimalFormat;

public class Constants {
  public static final String DRUID_RESPONSE_API_PATH = "/customer/{customerId}/bot/{botRef}/druid/response";

  public static final String REQ_PARAM_CUSTOMER_ID = "customerId";
  public static final String REQ_PARAM_BOT_REF = "botRef";
  public static final String TOP = "top";
  public static final String ORDER_COUNT = "order_count";
  public static final String SESSION_COUNT = "session_count";
  public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");
}
