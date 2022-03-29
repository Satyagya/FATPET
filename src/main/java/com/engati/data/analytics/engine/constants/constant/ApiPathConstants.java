package com.engati.data.analytics.engine.constants.constant;

public class ApiPathConstants {

  public static final String API_BASE_PATH = "/v1";
  public static final String GET_CUSTOMER_SEGMENT = "/customerId/{customerId}/botRef/{botRef}/segment/{segmentName}/getCustomerSegment";
  public static final String CUSTOMERID = "customerId";
  public static final String BOTREF = "botRef";
  public static final String SEGMENT_NAME = "segmentName";
  public static final String API_BASE_PATH_FOR_SEGMENT_DETAILS = "/customerId/{customerId}/botRef/{botRef}/segment/{segmentName}";
  public static final String API_BASE_PATH_FOR_VARIANTS_BY_UNIT_SALES = "/customerId/{customerId}/botRef/{botRef}/getVariantsByUnitSales";
  public static final String API_BASE_PATH_FOR_FETCHING_PURCHASE_HISTORY = "/getPurchaseHistory";




}
