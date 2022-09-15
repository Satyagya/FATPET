package com.engati.data.analytics.engine.constants.constant;

public class ApiPathConstants {

  public static final String API_BASE_PATH = "/v1";
  public static final String GET_SYSTEM_SEGMENT = "/botRef/{botRef}/segment/{segmentName}/getSystemSegment";
  public static final String GET_PRODUCTTYPE_COLLECTION_SEGMENT = "/botRef/{botRef}/segment/{segmentName}/getProductTypeCollectionSegment";
  public static final String CUSTOMERID = "customerId";
  public static final String BOTREF = "botRef";
  public static final String SEGMENT_NAME = "segmentName";
  public static final String API_BASE_PATH_FOR_SEGMENT_DETAILS = "/botRef/{botRef}/segment/{segmentName}";
  public static final String API_BASE_PATH_FOR_VARIANTS_BY_UNIT_SALES = "/botRef/{botRef}/getVariantsByUnitSales";
  public static final String API_BASE_PATH_FOR_FETCHING_PURCHASE_HISTORY = "/getPurchaseHistory";
  public static final String GET_CUSTOM_SEGMENT = "/botRef/{botRef}/getCustomSegment";
  public static final String GET_CUSTOM_SEGMENT_V2 = "/botRef/{botRef}/getCustomSegmentV2";
  public static final String SEGMENT_CONDITION = "segmentCondition";
  public static final String ENGAGED_USERS ="/{botRef}/getEngagedUsers";
  public static final String ORDER_COUNT ="/{botRef}/getOrderCount";
  public static final String GET_AOV = "/{botRef}/getAOV";
  public static final String GET_ABANDONED_CHECKOUTS = "/{botRef}/getAbandonedCheckouts";
  public static final String GET_TRANSACTIONS_VIA_ENGATI = "/{botRef}/getTransactionsFromEngati";
  public static final String GET_TRANSACTION_REVENUE_VIA_ENGATI = "/{botRef}/getTransactionRevenueFromEngati";
  public static final String MOST_PURCHASED_PRODUCTS = "/{botRef}/mostPurchasedProducts";
  public static final String MOST_ABANDONED_PRODUCTS = "/{botRef}/mostAbandonedProducts";
  public static final String BOT_QUERIES_CHART = "/{botRef}/botQueriesChart";
  public static final String ENGAGED_USERS_PER_PLATFORM ="{botRef}/getEngagedUsersPerPlatform";
  public static final String CONVERSATION_INTENT = "{botRef}/getConversationIntentBreakdown";
  public static final String CONVERSATION_SENTIMENT = "{botRef}/getConversationSentimentBreakdown";
  public static final String LAST_UPDATED_ON = "{botRef}/lastUpdatedOn";
  public static final String SHOPIFY_GOOGLE_ANALYTICS_INFO = "shopify_google_analytics_info";
  public static final String MANAGE_GA_CREDS = "shopify/dashboard/ga-creds";
  public static final String AUTH_JSON = "authJson";
  public static final String PROPERTY_ID = "propertyId";
 

}
