package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.Utils.EtlEngineRestUtility;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.QueryOperators;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.request.CustomSegmentRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationConfigurationResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationCustomSegmentResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.model.response.KafkaPayloadForSegmentStatus;
import com.engati.data.analytics.engine.repository.SegmentRepository;
import com.engati.data.analytics.engine.service.CustomerSegmentationConfigurationService;
import com.engati.data.analytics.engine.service.PrometheusManagementService;
import com.engati.data.analytics.engine.service.SegmentService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import retrofit2.Response;

import java.io.File;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Slf4j
@Service("com.engati.data.analytics.engine.service.SegmentsService")
public class SegmentServiceImpl implements SegmentService {

  @Autowired
  private CustomerSegmentationConfigurationService customerSegmentationConfigurationService;

  @Autowired
  private KafkaTemplate<String, String> kafka;

  @Autowired
  private SegmentRepository segmentRepository;

  @Autowired
  private CommonUtils commonUtils;

  @Value("${topic.shopify.segments.response}")
  private String segmentResponseTopic;

  @Value("${segment.remove.subscribedCust.shopDomains}")
  private String shopDomainsForSubscribedCustomers;

  @Value("${segment.subscription.titles}")
  private String subscriptionTitles;

  @Autowired
  private EtlEngineRestUtility etlEngineRestUtility;

  @Autowired
  @Qualifier("com.engati.data.analytics.engine.service.impl.PrometheusManagementServiceImpl")
  private PrometheusManagementService prometheusManagementService;

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private List<CustomerSegmentationResponse> getDetailsforCustomerSegments(Set<Long> customerList, Long botRef) {
    log.info("Getting details for Customer segment with botRef : {}", botRef);
    List<CustomerSegmentationResponse> customerSegmentationResponseList = new ArrayList<>();

    if(CollectionUtils.isEmpty(customerList)) {
      return customerSegmentationResponseList;
    }

    log.info("Customer List:{}",customerList);

    Map<Long,Map<String,String>> customerDetails = getCustomerDetails(customerList, botRef);

    log.info("Customer Details of Segment:{}",customerDetails);

    String storeAOV = null;
    storeAOV = getStoreAOV(botRef);
    Map<Long, Map<String, Object>> customerAOV = getCustomerAOV(customerList, botRef);
    Map<Long, Map<String, Long>> ordersLastMonth = getOrdersForLastXMonth(customerList, botRef, 1);
    Map<Long, Map<String, Long>> ordersLast6Months = getOrdersForLastXMonth(customerList, botRef, 6);
    Map<Long, Map<String, Long>> ordersLast12Months = getOrdersForLastXMonth(customerList, botRef, 12);
    for (Long customerId : customerList) {
      CustomerSegmentationResponse customerSegmentationResponse = new CustomerSegmentationResponse();

      customerSegmentationResponse.setCustomerEmail(Constants.DEFAULT_EMAIL);
      customerSegmentationResponse.setCustomerPhone(Constants.DEFAULT_PHONE);
      customerSegmentationResponse.setCustomerName(Constants.DEFAULT_NAME);

      if(Objects.nonNull(customerDetails) && Objects.nonNull(customerDetails.get(customerId))) {
        customerSegmentationResponse.setCustomerEmail(customerDetails.get(customerId).getOrDefault(Constants.CUSTOMER_EMAIL,Constants.DEFAULT_EMAIL));
        customerSegmentationResponse.setCustomerPhone(customerDetails.get(customerId).getOrDefault(Constants.CUSTOMER_PHONE,Constants.DEFAULT_PHONE));
        customerSegmentationResponse.setCustomerName(customerDetails.get(customerId).getOrDefault(Constants.CUSTOMER_NAME,Constants.DEFAULT_NAME));
      }

      log.info("customerId:{}",customerId);
      log.info("customerName:{}", customerSegmentationResponse.getCustomerName());
      log.info("customerEmail:{}",customerSegmentationResponse.getCustomerEmail());
      log.info("customerPhone:{}",customerSegmentationResponse.getCustomerPhone());

      customerSegmentationResponse.setStoreAOV(Double.valueOf(storeAOV));
      try {
        customerSegmentationResponse.setCustomerAOV((Double) customerAOV.get(customerId).getOrDefault(QueryConstants.AOV, Constants.DEFAULT_AOV_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setCustomerAOV(Double.valueOf(Constants.DEFAULT_AOV_VALUE));
      }

      try {
        customerSegmentationResponse.setOrdersInLastOneMonth(ordersLastMonth.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_1_MONTH, Long.valueOf(Constants.DEFAULT_ORDER_VALUE)));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastOneMonth(Long.valueOf(Constants.DEFAULT_ORDER_VALUE));
      }
      try {
        customerSegmentationResponse.setOrdersInLastSixMonths(ordersLast6Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_6_MONTHS, Long.valueOf(Constants.DEFAULT_ORDER_VALUE)));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastSixMonths(Long.valueOf(Constants.DEFAULT_ORDER_VALUE));
      }
      try {
        customerSegmentationResponse.setOrdersInLastTwelveMonths(ordersLast12Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_12_MONTHS, Long.valueOf(Constants.DEFAULT_ORDER_VALUE)));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastTwelveMonths(Long.valueOf(Constants.DEFAULT_ORDER_VALUE));
      }
      customerSegmentationResponseList.add(customerSegmentationResponse);
      customerSegmentationResponseList.sort(Collections.reverseOrder());
    }
    return customerSegmentationResponseList;
  }

  private List<CustomerSegmentationCustomSegmentResponse> getDetailsforCustomerCustomSegments(Set<Long> customerList, Long botRef,String startDate,String endDate) {
    log.info("Getting details for Customer segment with botRef : {}", botRef);
    List<CustomerSegmentationCustomSegmentResponse> customerSegmentationResponseList = new ArrayList<>();

    if(CollectionUtils.isEmpty(customerList)) {
      return customerSegmentationResponseList;
    }

    Map<Long,Map<String,String>> customerDetails = getCustomerDetails(customerList, botRef);

    String storeAOV = null;
    storeAOV = getStoreAOV(botRef);
    Map<Long,Map<String,Long>> customerOrders = getCustomerOrdersCustomSegment(customerList,botRef,startDate,endDate);
    Map<Long,Map<String,Object>> customerAOV = getCustomerAOVCustomSegment(customerList,botRef,startDate,endDate);
    Map<Long,Map<String,Object>> customerAmountSpent = getCustomerAmountSpentCustomSegment(customerList,botRef,startDate,endDate);
    Map<Long,Map<String,Date>> customerLastOrderDate = getCustomerLastOrderDateCustomSegment(customerList,botRef,startDate,endDate) ;
    Map<Long,Map<String,String>> customerProductType = getCustomerProductTypeCustomSegment(customerList,botRef,startDate,endDate);
    for (Long customerId : customerList) {
      CustomerSegmentationCustomSegmentResponse customerSegmentationCustomSegmentResponse = new CustomerSegmentationCustomSegmentResponse();

      customerSegmentationCustomSegmentResponse.setCustomerEmail(Constants.DEFAULT_EMAIL);
      customerSegmentationCustomSegmentResponse.setCustomerPhone(Constants.DEFAULT_PHONE);
      customerSegmentationCustomSegmentResponse.setCustomerName(Constants.DEFAULT_NAME);

      if(Objects.nonNull(customerDetails) && Objects.nonNull(customerDetails.get(customerId))) {
        customerSegmentationCustomSegmentResponse.setCustomerEmail(customerDetails.get(customerId).getOrDefault(Constants.CUSTOMER_EMAIL,Constants.DEFAULT_EMAIL));
        customerSegmentationCustomSegmentResponse.setCustomerPhone(customerDetails.get(customerId).getOrDefault(Constants.CUSTOMER_PHONE,Constants.DEFAULT_PHONE));
        customerSegmentationCustomSegmentResponse.setCustomerName(customerDetails.get(customerId).getOrDefault(Constants.CUSTOMER_NAME,Constants.DEFAULT_NAME));
      }

      customerSegmentationCustomSegmentResponse.setStoreAOV(Double.valueOf(storeAOV));

      try {
        customerSegmentationCustomSegmentResponse.setCustomerOrders(customerOrders.get(customerId).getOrDefault(QueryConstants.TOTAL_ORDERS, Long.valueOf(Constants.DEFAULT_ORDER_VALUE)));
      } catch (NullPointerException e) {
        customerSegmentationCustomSegmentResponse.setCustomerOrders(Long.valueOf(Constants.DEFAULT_ORDER_VALUE));
      }

      try {
        customerSegmentationCustomSegmentResponse.setCustomerAOV((Double) customerAOV.get(customerId).getOrDefault(QueryConstants.AOV, Constants.DEFAULT_AOV_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationCustomSegmentResponse.setCustomerAOV(Double.valueOf(Constants.DEFAULT_AOV_VALUE));
      }

      try {
        customerSegmentationCustomSegmentResponse.setCustomerAmountSpent((Double) customerAmountSpent.get(customerId).getOrDefault(QueryConstants.AMOUNT_SPENT, Constants.DEFAULT_AMOUNT_SPENT_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationCustomSegmentResponse.setCustomerAmountSpent(Double.valueOf(Constants.DEFAULT_AMOUNT_SPENT_VALUE));
      }

      try {
        customerSegmentationCustomSegmentResponse.setCustomerLastOrderDate(customerLastOrderDate.get(customerId).getOrDefault(QueryConstants.LAST_ORDER_DATE, Constants.DEFAULT_LAST_ORDER_DATE));
      } catch (NullPointerException e) {
        customerSegmentationCustomSegmentResponse.setCustomerLastOrderDate(Constants.DEFAULT_LAST_ORDER_DATE);
      }

      try {
        customerSegmentationCustomSegmentResponse.setCustomerProductTypes(customerProductType.get(customerId).getOrDefault(QueryConstants.PRODUCT_TYPES, Constants.DEFAULT_PRODUCT_TYPES));
      } catch (NullPointerException e) {
        customerSegmentationCustomSegmentResponse.setCustomerProductTypes(Constants.DEFAULT_PRODUCT_TYPES);
      }

      customerSegmentationResponseList.add(customerSegmentationCustomSegmentResponse);
      customerSegmentationResponseList.sort(Collections.reverseOrder());
    }
    return customerSegmentationResponseList;
  }

  private Map<Long,Map<String,String>> getCustomerDetails(Set<Long> customerIds, Long botRef) {
    Map<Long,Map<String,String>> customerDetails = new HashMap<>();
    try{
      String query = NativeQueries.GET_CUSTOMER_DETAILS;
      query = query.replace(Constants.BOT_REF,botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerDetails = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)),new TypeReference<Map<Long,Map<String,String>>>(){
        });
      }
    } catch (Exception e) {
      log.error("Error while getting Customer Details for: botRef:{}", botRef, e);
    }
    return customerDetails;
  }

  private Map<Long,Map<String,Date>> getCustomerLastOrderDateCustomSegment(Set<Long> customerIds, Long botRef,String startDate,String endDate) {
    Map<Long,Map<String,Date>> customerLastOrderDate = new HashMap<>();
    try {
      String query = NativeQueries.CUSTOMER_LAST_ORDER_DATE;
      query = query.replace(Constants.BOT_REF,botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      query = query.replace(QueryConstants.START_DATE,startDate);
      query = query.replace(QueryConstants.END_DATE,endDate);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerLastOrderDate = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Date>>>() {
        });
      }

    } catch (Exception e) {
      log.error("Error while getting Customer Last Order Date for: botRef:{}", botRef, e);
    }
    return customerLastOrderDate;
  }

  private Map<Long,Map<String,Object>> getCustomerAmountSpentCustomSegment(Set<Long> customerIds, Long botRef, String startDate, String endDate) {
    Map<Long,Map<String,Object>> customerSpend = new HashMap<>();
    try {
      String query = NativeQueries.CUSTOMER_AMOUNT_SPENT;
      query = query.replace(Constants.BOT_REF,botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      query = query.replace(QueryConstants.START_DATE,startDate);
      query = query.replace(QueryConstants.END_DATE,endDate);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerSpend = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Object>>>() {
        });
      }

    } catch (Exception e) {
      log.error("Error while getting Customer Spend for: botRef:{}", botRef, e);
    }
    return customerSpend;
  }

  private Map<Long,Map<String,String>> getCustomerProductTypeCustomSegment(Set<Long> customerIds, Long botRef,String startDate,String endDate) {
    Map<Long,Map<String,String>> customerProductType = new HashMap<>();
    try{
      String query = NativeQueries.CUSTOMER_PRODUCT_TYPE;
      query = query.replace(Constants.BOT_REF,botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      query = query.replace(QueryConstants.START_DATE,startDate);
      query = query.replace(QueryConstants.END_DATE,endDate);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerProductType = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, String>>>() {
        });
      }


    } catch (Exception e) {
      log.error("Error while getting Customer Product Type for: botRef:{}", botRef, e);
    }

    return customerProductType;
  }

  @Override
  public DataAnalyticsResponse<List<CustomerSegmentationResponse>> getCustomersForSystemSegment(Long botRef, String segmentName) {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, segment: {}", botRef, segmentName);

    KafkaPayloadForSegmentStatus kafkaPayload = new KafkaPayloadForSegmentStatus();
    kafkaPayload.setSegmentName(segmentName);
    kafkaPayload.setBotRef(botRef);
    kafkaPayload.setSegmentType(Constants.SYSTEM_SEGMENT);

    DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = new DataAnalyticsResponse<>();
    response.setStatus(ResponseStatusCode.SUCCESS);
    DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails = customerSegmentationConfigurationService.getSystemSegmentConfigByBotRefAndSegment(botRef, segmentName);
    log.info("Checking for Config values for the segment values for {} for botRef: {}", segmentName, botRef);
    Set<Long> resultSet = null;
    Set<Long> recencySegment = null;
    Set<Long> subscriptionOrdersSegment = null;
    if (configDetails.getResponseObject().getRecencyMetric() != null) {
      log.info("Recency configurations found for botRef: {}, segmentName {}", botRef, segmentName);
      recencySegment = getRecencySegment(configDetails);
    }
    Set<Long> frequencySegment = null;
    if (configDetails.getResponseObject().getFrequencyMetric() != null) {
      log.info("Frequency configurations found for botRef: {}, segmentName {}", botRef, segmentName);
      frequencySegment = getFrequencySegment(configDetails);
    }
    Set<Long> monetarySegment = null;
    if (configDetails.getResponseObject().getMonetaryMetric() != null) {
      log.info("Monetary configurations found for botRef: {}, segmentName {}", botRef, segmentName);
      monetarySegment = getMonetarySegment(configDetails);
    }
    resultSet = getIntersectionForSegments(recencySegment, frequencySegment, monetarySegment);
    List<String> shopDomains_subscription =
        Arrays.asList(shopDomainsForSubscribedCustomers.split(","));
    String shopDomainForBotRef = segmentRepository.findShopDomainByBotRef(botRef);
    if (shopDomainForBotRef!=null && shopDomains_subscription.stream()
        .anyMatch(shopDomainForBotRef::contains)
        && resultSet != null) {
      subscriptionOrdersSegment = getSubsOrderSegment(botRef);
      resultSet.removeAll(subscriptionOrdersSegment);
    }
    try {
      String fileName = getOutputFileName(botRef, segmentName);
      kafkaPayload.setFileName(fileName);
      if (Objects.nonNull(fileName)) {
        kafkaPayload.setStatus("SUCCESS");
        kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
        if (!CollectionUtils.isEmpty(resultSet)) {
          List<CustomerSegmentationResponse> customerDetail = getDetailsforCustomerSegments(resultSet, botRef);
          kafkaPayload.setCustomerCount(customerDetail.size());
          if (!CommonUtils.createCsv(customerDetail, botRef, segmentName, fileName)) {
            response.setStatus(ResponseStatusCode.CSV_CREATION_EXCEPTION);
            kafkaPayload.setStatus("FAILURE - CSV_CREATION_FAILURE");
            kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
          }
        } else {
          List<CustomerSegmentationResponse> customerDetail = getDetailsforCustomerSegments(resultSet,botRef);
          kafkaPayload.setCustomerCount(customerDetail.size());
          if (!CommonUtils.createCsv(customerDetail, botRef, segmentName, fileName)) {
            response.setStatus(ResponseStatusCode.CSV_CREATION_EXCEPTION);
            kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
            kafkaPayload.setStatus("FAILURE - CSV_CREATION_FAILURE");
          } else {
            response.setStatus(ResponseStatusCode.EMPTY_SEGMENT);
            kafkaPayload.setStatus("SUCCESS - EMPTY_CSV");
            kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
          }
        }
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getCustomersForSystemSegment", botRef,
          e.getMessage(), segmentName);
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Exception caught while getting customer details for botRef: {}, segment: {}", botRef, segmentName, e);
      kafkaPayload.setStatus("FAILURE - PROCESSING ERROR");
      kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
    }
    try {
      log.info("Pushing to the response kafka topic, payload : {}", kafkaPayload);
      kafka.send(segmentResponseTopic, CommonUtils.MAPPER.writeValueAsString(kafkaPayload));
    } catch (JsonProcessingException e) {
      log.error("Error publishing message to kafka for kafkaPayload: {}", kafkaPayload, e);
    }
    return response;
  }

  private Set<Long> getSubsOrderSegment(Long botRef) {
    Set<Long> subscriptionOrdersList = new HashSet<>();
    JSONObject requestBody = new JSONObject();
    List<String> subscriptionTitlesList =
        Arrays.asList(subscriptionTitles.split(","));
    try {
      String query = NativeQueries.SUBSCRIPTION_ORDER_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.SUBSCRIPTION_TITLES,
          subscriptionTitlesList.stream().collect(Collectors.joining("','", "'", "'")));
      requestBody.put(Constants.QUERY, query);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(
          etlResponse.body())) {
        subscriptionOrdersList = (Set<Long>) MAPPER.readValue(MAPPER.writeValueAsString(
                    etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.CUSTOMER_ID)),
                ArrayList.class).stream().map(x -> ((Number) x).longValue())
            .collect(Collectors.toSet());
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getSubscriptionOrderSegment", botRef,
          e.getMessage(), requestBody.toString());
      log.error("Exception while getting Subscription orders for botRef: {}", botRef.toString(), e);
    }
    return subscriptionOrdersList;
  }

  private String getOutputFileName(Long botRef, String segmentName) {
    String csvBasePath = String.format(Constants.CSV_BASE_PATH_FORMAT, botRef);
    String fileName = String.format(Constants.CSV_PATH_FORMAT, botRef, segmentName);
    File file = new File(csvBasePath);
    if (!file.exists() && !file.mkdirs()) {
      log.error("Unable to create customer-segment directory for botRef: {} for segmentName: {}", botRef, segmentName);
      return null;
    }
    return fileName;
  }

  private Set<Long> getIntersectionForSegments(Set<Long> recencySet, Set<Long> frequencySet, Set<Long> monetarySet) {
    List<Set<Long>> listSegment = new ArrayList<>();
    if (!CollectionUtils.isEmpty(recencySet)) listSegment.add(recencySet);
    if (!CollectionUtils.isEmpty(frequencySet)) listSegment.add(frequencySet);
    if (!CollectionUtils.isEmpty(monetarySet)) listSegment.add(monetarySet);

    if (listSegment.size() == 0) return null;
    else if (listSegment.size() == 1) return listSegment.get(0);
    else if (listSegment.size() == 2) {
      Set<Long> resSet1 = listSegment.get(0);
      Set<Long> resSet2 = listSegment.get(1);
      resSet1.retainAll(resSet2);
      return resSet1;
    } else {
      Set<Long> resSet1 = listSegment.get(0);
      Set<Long> resSet2 = listSegment.get(1);
      Set<Long> resSet3 = listSegment.get(2);
      resSet1.retainAll(resSet2);
      resSet1.retainAll(resSet3);
      return resSet1;
    }
  }

  private Set<Long> getMonetarySegment(DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails) {
    Set<Long> monetaryList = new HashSet<>();
    try {
      String query = NativeQueries.MONETARY_QUERY;
      query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getMonetaryOperator()));
      query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
      query = query.replace(QueryConstants.METRIC, configDetails.getResponseObject().getMonetaryMetric());
      if (configDetails.getResponseObject().getMonetaryValue().equals(QueryConstants.STORE_AOV)) {
        query = query.replace(QueryConstants.VALUE, getStoreAOV(configDetails.getResponseObject().getBotRef()));
      }

      query = query.replace(QueryConstants.VALUE, configDetails.getResponseObject().getMonetaryValue());
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        monetaryList = (Set<Long>) MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.CUSTOMER_ID)), ArrayList.class).stream().map(x -> ((Number) x).longValue()).collect(Collectors.toSet());
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getMonetarySegment", configDetails.getResponseObject().getBotRef(), e.getMessage(),
          CommonUtils.getStringValueFromObject(configDetails));
      log.error("Exception while getting StoreAOV for botRef: {}", configDetails.getResponseObject().getBotRef().toString(), e);
    }
    return monetaryList;
  }

  private Set<Long> getFrequencySegment(DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails) {
    Set<Long> frequencyList = new HashSet<>();
    try {
      String query = NativeQueries.FREQUENCY_QUERY;
      query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getFrequencyOperator()));
      query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
      query = query.replace(QueryConstants.GAP, configDetails.getResponseObject().getFrequencyValue().toString());
      query = query.replace(QueryConstants.ORDERS_CONFIGURED, configDetails.getResponseObject().getFrequencyMetric().toString());
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        frequencyList = (Set<Long>) MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.CUSTOMER_ID)), ArrayList.class).stream().map(x -> ((Number) x).longValue()).collect(Collectors.toSet());
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getFrequencySegment", configDetails.getResponseObject().getBotRef(), e.getMessage(),
          CommonUtils.getStringValueFromObject(configDetails));
      log.error("Error while getting Frequency Segment for: {}", configDetails, e);
    }
    return frequencyList;
  }

  private Set<Long> getRecencySegment(DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails) {
    Set<Long> recencyList = new HashSet<>();
    try {
      String query = NativeQueries.RECENCY_QUERY;
      query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getRecencyOperator()));
      if (configDetails.getResponseObject().getRecencyMetric().equals(QueryConstants.LAST_ORDER_DATE)) {
        query = query.replace(QueryConstants.AGGREGATOR, QueryConstants.MAX);
      } else {
        query = query.replace(QueryConstants.AGGREGATOR, QueryConstants.MIN);
      }
      query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
      query = query.replace(QueryConstants.GAP, configDetails.getResponseObject().getRecencyValue().toString());
      query = query.replace(QueryConstants.COLUMN_NAME, configDetails.getResponseObject().getRecencyMetric().toLowerCase(Locale.ROOT));
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        recencyList = (Set<Long>) MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.CUSTOMER_ID)), ArrayList.class).stream().map(x -> ((Number) x).longValue()).collect(Collectors.toSet());
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getRecencySegment", configDetails.getResponseObject().getBotRef(), e.getMessage(),
          CommonUtils.getStringValueFromObject(configDetails));
      log.error("Error while getting Recency Segment for:{}", configDetails, e);
    }
    return recencyList;
  }


  private String getStoreAOV(Long botRef) {
    String storeAov = "0.0";
    try {
      String query = NativeQueries.STORE_AOV_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      log.info(query);
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        String responseString = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body()), JsonNode.class).get(Constants.RESPONSE_OBJECT).get(QueryConstants.STORE_AOV).toString();

        storeAov = String.valueOf(MAPPER.readValue(responseString, ArrayList.class).stream().findFirst().get());
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getStoreAOV", botRef, e.getMessage(), "");
      log.error("Exception while getting StoreAOV for botRef: {}", botRef, e);
    }
    return storeAov;
  }

  private Map<Long,Map<String, Long>> getCustomerOrdersCustomSegment(Set<Long> customerIds, Long botRef,String startDate,String endDate) {
    Map<Long, Map<String, Long>> customerOrders = new HashMap<>();
    try {
      String query = NativeQueries.CUSTOMER_ORDERS;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      query = query.replace(QueryConstants.START_DATE,startDate);
      query = query.replace(QueryConstants.END_DATE,endDate);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerOrders = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Long>>>() {
        });
      }
    } catch (Exception e) {
      log.error("Error while getting Customer Orders for: botRef:{}", botRef, e);
    }
    return customerOrders;
  }

  private Map<Long,Map<String,Object>> getCustomerAOVCustomSegment(Set<Long> customerIds, Long botRef,String startDate,String endDate) {
    Map<Long,Map<String,Object>> customerAOV = new HashMap<>();
    try {
      String query = NativeQueries.CUSTOMER_AOV_CUSTOM_SEGMENT;
      query = query.replace(Constants.BOT_REF,botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
      query = query.replace(QueryConstants.START_DATE,startDate);
      query = query.replace(QueryConstants.END_DATE,endDate);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerAOV = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Object>>>() {
        });
      }

    } catch (Exception e) {
      log.error("Error while getting Customer AOV for: botRef:{}", botRef, e);
    }
    return customerAOV;
  }

  private Map<Long, Map<String, Object>> getCustomerAOV(Set<Long> customerIds, Long botRef) {
    Map<Long, Map<String, Object>> customerAOV = new HashMap<>();
    try {
      String query = NativeQueries.CUSTOMER_AOV_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerAOV = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Object>>>() {
        });
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getCustomerAOV", botRef, e.getMessage(),
          "");
      log.error("Error while getting Customer AOV for: botRef:{}", botRef, e);
    }
    return customerAOV;
  }

  private Map<Long, Map<String, Long>> getOrdersForLastXMonth(Set<Long> customerIds, Long botRef, Integer Month) {
    Map<Long, Map<String, Long>> customerOrders = new HashMap<>();
    try {

      String query = NativeQueries.ORDERS_FOR_X_MONTHS;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      query = query.replace(QueryConstants.GAP, Month.toString());
      query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
      if (Month == 1) query = query.replace(QueryConstants.MONTHS, QueryConstants.MONTH);
      query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
      query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);

      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      requestBody.put(Constants.KEY, Constants.CUSTOMER_ID);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JSONObject> etlResponse = etlEngineRestUtility.executeQueryDetails(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        customerOrders = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Long>>>() {
        });
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getOrdersForLastXMonth", botRef,
          e.getMessage(), CommonUtils.getStringValueFromObject(customerIds));
      log.error("Error while getting Orders For Last X Month for botRef: {}", botRef, e);
    }
    return customerOrders;
  }

  @Override
  public DataAnalyticsResponse<List<CustomerSegmentationCustomSegmentResponse>> getCustomersForCustomSegment(Long botRef, CustomSegmentRequest customSegmentRequest) {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, customSegmentRequest: {}", botRef, customSegmentRequest);

    String segmentCondition = customSegmentRequest.getSegmentCondition();
    String segmentName = customSegmentRequest.getSegmentName();

    KafkaPayloadForSegmentStatus kafkaPayload = new KafkaPayloadForSegmentStatus();
    kafkaPayload.setSegmentName(customSegmentRequest.getSegmentName());
    kafkaPayload.setFileName(customSegmentRequest.getFileName());
    kafkaPayload.setBotRef(botRef);
    kafkaPayload.setSegmentType(Constants.CUSTOM_SEGMENT);

    DataAnalyticsResponse<List<CustomerSegmentationCustomSegmentResponse>> response = new DataAnalyticsResponse<>();
    response.setStatus(ResponseStatusCode.SUCCESS);

    DateFormat formatter = new SimpleDateFormat(Constants.DATE_FORMAT);

    String startDate="";
    String endDate="";

    if(Objects.nonNull(customSegmentRequest.getDateRange())) {

      ArrayList<Date> dateRange = customSegmentRequest.getDateRange();

      if(dateRange.size()!=2 )
      {
        response.setResponseObject(null);
        response.setStatus(ResponseStatusCode.DATE_RANGE_IS_NOT_VALID);
        kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
        kafkaPayload.setStatus("FAILURE - DATE_RANGE_IS_NOT_VALID");
        kafka.send(segmentResponseTopic, kafkaPayload.toString());
        return response;
      }

      startDate = formatter.format(dateRange.get(0));
      endDate = formatter.format(dateRange.get(1));

      DateFormat payLoadDateFormat = new SimpleDateFormat(Constants.PAYLOAD_DATE_FORMAT);
      String payLoadStartDate = payLoadDateFormat.format(dateRange.get(0));
      String payLoadEndDate = payLoadDateFormat.format(dateRange.get(1));

      String payLoadDateSelected = payLoadStartDate + ',' +payLoadEndDate;
      kafkaPayload.setDateRange(payLoadDateSelected);
    } else {
      response.setResponseObject(null);
      response.setStatus(ResponseStatusCode.DATE_RANGE_IS_NOT_VALID);
      kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
      kafkaPayload.setStatus("FAILURE - DATE_RANGE_IS_NOT_VALID");
      kafka.send(segmentResponseTopic, kafkaPayload.toString());
      return response;
    }

    Pattern segmentOperators = Pattern.compile("(?i) AND | OR ");
    Matcher OperatorMatcher = segmentOperators.matcher(segmentCondition);
    ArrayList<String> operators = new ArrayList<>();

    while (OperatorMatcher.find()) {
      operators.add(OperatorMatcher.group().trim());
    }

    if (operators.size() > Constants.MAXIMUM_NUMBER_OF_OPERATORS) {
      response.setResponseObject(null);
      response.setStatus(ResponseStatusCode.OPERATORS_PERMISSIBLE_LIMITS_REACHED);
      kafkaPayload.setStatus("FAILURE - EXCEEDED_OPERATOR_LIMITS");
      kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
      kafka.send(segmentResponseTopic, kafkaPayload.toString());
      return response;
    }

    String[] operands = segmentCondition.split("(?i) AND | OR ");
    String query_for_operand = "";

    Set<Long> customerId = null;
    Set<Long> resultSet = null;

    Iterator it = operators.iterator();

    for(int index=0;index<operands.length;index++) {
      String operand = operands[index];

      if(operand.contains("ORDERS")) {
        query_for_operand = generateQueryForOrdersCustomSegment(botRef,operand,startDate,endDate);

      } else if(operand.contains("AVERAGE_ORDER_VALUE")) {
        query_for_operand = generateQueryForCustomerAOVCustomSegment(botRef,operand,startDate,endDate);

      } else if(operand.contains("LAST_ORDER")) {
        query_for_operand = generateQueryForLastOrderDaysCustomSegment(botRef,operand,startDate,endDate);

      } else if(operand.contains("AMOUNT_SPENT")) {
        query_for_operand = generateQueryForAmountSpentCustomSegment(botRef,operand,startDate,endDate);

      } else if(operand.contains("PRODUCT_TYPE")) {
        String productType = operand.split("IN", 2)[1];
        Set<String> productTypes = Arrays.stream(productType.split(",")).map(str -> str.trim()).collect(Collectors.toSet());
        query_for_operand = generateQueryForProductTypeCustomSegment(botRef,operand,startDate,endDate,productTypes);

      } else if(operand.contains("CITY")) {
        String inputCity = operand.split("IN", 2)[1];
        Set<String> cityList = Arrays.stream(inputCity.split(",")).map(str -> str.trim()).collect(Collectors.toSet());
//        List<String> customerCity = getCity(botRef);
//
//        cityList = cityList.stream()
//                .map(str->str.toLowerCase())
//                .map(str->str.replace(" ",""))
//                .collect(Collectors.toList());
//
//        customerCity = customerCity.stream()
//                .map(str->str.toLowerCase())
//                .map(str->str.replace(" ",""))
//                .collect(Collectors.toList());
//
////        Set<String> cityListForQuery = new HashSet<>();
////
//        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();
//
////        List<String> finalCityList = cityList;
//        List<String> finalCityList = cityList;
//        Set<String> cityListForQuery = customerCity.parallelStream()
//                .distinct()
//                .filter(city -> finalCityList.stream()
//                        .anyMatch(city1 ->
//                                levenshteinDistance.apply(city1,city)<=2))
//                .collect(Collectors.toSet());
//
//        log.info("CityList:{}",cityListForQuery);
////
////        for(String city:cityList) {
////            for(String parquetCity : customerCity) {
////              if(levenshteinDistance.apply(city,parquetCity)<=2) {
////                cityListForQuery.add('\'' + parquetCity + '\'');
////              }
////            }
////        }
//

        query_for_operand = generateQueryForCityCustomSegment(botRef,operand,startDate,endDate,cityList);

      }  else if(operand.contains("COUNTRY")) {
         String inputCountry = operand.split("IN",2)[1];
         Set<String> countryList = Arrays.stream(inputCountry.split(",")).map(str->str.trim()).collect(Collectors.toSet());
         query_for_operand = generateQueryForCountryCustomSegment(botRef, operand, startDate, endDate, countryList);

      }else if(operand.contains("COLLECTION")) {
        String inputCollection = operand.split("IN",2)[1];
        Set<String> collectionList = Arrays.stream(inputCollection.split(",")).map(str -> str.trim()).collect(Collectors.toSet());
        query_for_operand = generateQueryForCollectionCustomSegment(botRef, operand, startDate, endDate, collectionList);

      } else {
        response.setResponseObject(null);
        response.setStatus(ResponseStatusCode.INVALID_ATTRIBUTES_PROVIDED);
        kafkaPayload.setStatus("FAILURE - INVALID_ATTRIBUTES");
        kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
        kafka.send(segmentResponseTopic, kafkaPayload.toString());
        return response;
      }

      Map<String, Object> query_operand_parameter_response = getCustomerListForParameter(query_for_operand);
      customerId = (Set<Long>) query_operand_parameter_response.get("response");
      Long query_for_operand_execution_time = (Long) query_operand_parameter_response.get("execution_time");
      log.info("Executed Query: {} in time: {}", query_for_operand, query_for_operand_execution_time);

      if(Objects.isNull(resultSet)) {
          resultSet = customerId;
      }
      else {

        String condition="";

        if(it.hasNext()) {
            condition = it.next().toString().toUpperCase();
        }
        else {
          response.setResponseObject(null);
          response.setStatus(ResponseStatusCode.INVALID_TOTAL_NUMBER_OF_CONDITIONS);
          return response;
        }

        if(condition.compareTo("AND")==0) {
          resultSet.retainAll(customerId);

        } else if(condition.compareTo("OR")==0) {
          resultSet.addAll(customerId);

        } else {
          response.setResponseObject(null);
          response.setStatus(ResponseStatusCode.INVALID_EXPRESSION_CONDITION_PROVIDED);
          kafkaPayload.setStatus("FAILURE - INVALID_EXPRESSION_CONDITION_PROVIDED");
          kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
          kafka.send(segmentResponseTopic, kafkaPayload.toString());
          return response;
        }

      }

    }
    List<String> shopDomains_subscription =
        Arrays.asList(shopDomainsForSubscribedCustomers.split(","));
    String shopDomainForBotRef = segmentRepository.findShopDomainByBotRef(botRef);
    if (shopDomainForBotRef!=null && shopDomains_subscription.stream()
        .anyMatch(shopDomainForBotRef::contains)
        && resultSet != null) {
      Set<Long> subscriptionOrdersSegment = getSubsOrderSegment(botRef);
      resultSet.removeAll(subscriptionOrdersSegment);
    }

    try {
      String fileName = getOutputFileName(botRef, customSegmentRequest.getFileName());
      if (Objects.nonNull(fileName)) {
        kafkaPayload.setStatus("SUCCESS");
        kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
        if (!CollectionUtils.isEmpty(resultSet)) {
          List<CustomerSegmentationCustomSegmentResponse> customerDetail = getDetailsforCustomerCustomSegments(resultSet,botRef,startDate,endDate);
          kafkaPayload.setCustomerCount(customerDetail.size());
          if (!CommonUtils.createCustomSegmentCsv(customerDetail, botRef, segmentName, fileName)) {
            response.setStatus(ResponseStatusCode.CSV_CREATION_EXCEPTION);
            kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
            kafkaPayload.setStatus("FAILURE - CSV_CREATION_FAILURE");
          }
        } else {
          List<CustomerSegmentationCustomSegmentResponse> customerDetail = getDetailsforCustomerCustomSegments(resultSet,botRef,startDate,endDate);
          kafkaPayload.setCustomerCount(customerDetail.size());
          if (!CommonUtils.createCustomSegmentCsv(customerDetail, botRef, segmentName, fileName)) {
            response.setStatus(ResponseStatusCode.CSV_CREATION_EXCEPTION);
            kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
            kafkaPayload.setStatus("FAILURE - CSV_CREATION_FAILURE");
          } else {
            response.setStatus(ResponseStatusCode.EMPTY_SEGMENT);
            kafkaPayload.setStatus("SUCCESS - EMPTY_CSV");
            kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
          }
        }
      }
    } catch (Exception e) {
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Exception caught while getting customer details for botRef: {}, segment: {}", botRef, segmentName, e);
      kafkaPayload.setTimestamp(Timestamp.from(Instant.now()));
      kafkaPayload.setStatus("FAILURE - PROCESSING ERROR");
    }
    try {
      log.info("Pushing to the response kafka topic, payload : {}", kafkaPayload);
      kafka.send(segmentResponseTopic, CommonUtils.MAPPER.writeValueAsString(kafkaPayload));
    } catch (JsonProcessingException e) {
      log.error("Error publishing message to kafka for kafkaPayload: {}", kafkaPayload, e);
    }
    return response;
  }

  public Map<String, Object> getCustomerListForParameter(String parameter_query_definition) {
    JSONObject requestBody = new JSONObject();
    requestBody.put(Constants.QUERY, parameter_query_definition);
    Set<Long> parameter_customer_set = null;
    Map<String, Object> responseMap = new HashMap<>();
    try {
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        parameter_customer_set = (Set<Long>) MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT).get(Constants.CUSTOMER_ID)), ArrayList.class).stream().map(x -> ((Number) x).longValue()).collect(Collectors.toSet());
        responseMap.put("execution_time", etlResponse.raw().receivedResponseAtMillis() - etlResponse.raw().sentRequestAtMillis());
        responseMap.put("response", parameter_customer_set);
      }
    } catch (Exception e) {
      prometheusManagementService.apiRequestFailureEvent("getCustomerListForParameter", 0L,
          e.getMessage(), parameter_query_definition);
      e.printStackTrace();
    }
    return responseMap;
  }

  public String generateQueryForOrdersInLastXMonthsWithFilters(String operand, Long botRef) {
    String query = NativeQueries.ORDERS_FOR_X_MONTH_WITH_FILTERS;
    String[] operand_params = operand.split(" ");
    query = query.replace(QueryConstants.OPERATOR, operand_params[1]);
    query = query.replace(QueryConstants.VALUE, operand_params[2]);
    query = query.replace(Constants.BOT_REF, botRef.toString());
    if (operand.contains("ONE")) {
      query = query.replace(QueryConstants.GAP, "1");
    } else if (operand.contains("SIX")) {
      query = query.replace(QueryConstants.GAP, "6");
    } else if (operand.contains("TWELVE")) {
      query = query.replace(QueryConstants.GAP, "12");
    } else {
      return query = "";
    }
    return query;
  }

  public String generateQueryForCustomerAOVWithFilters(String operand, Long botRef) {
    String query = NativeQueries.CUSTOMER_AOV_QUERY_WITH_FILTERS;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    String[] operand_params = operand.split(" ");
    if (operand_params.length == 3) {
      query = query.replace(QueryConstants.OPERATOR, operand_params[1]);
      if (operand_params[2].compareTo("STORE_AOV") == 0) {
        query = query.replace(QueryConstants.VALUE, getStoreAOV(botRef));
      } else {
        query = query.replace(QueryConstants.VALUE, operand_params[2]);
      }
      query = query.replace(QueryConstants.GAP, "3"); // gap is defaulted for AOV related queries to 3 months
      return query;
    } else {
      log.error("Error while generating query for Orders For CustomerAOV for botRef: {}", botRef);
      return "";
    }
  }

  public String generateQueryForCustomerAOVCustomSegment(Long botRef, String operand, String startDate,String endDate) {
    String query = NativeQueries.CUSTOMER_AOV_QUERY_CUSTOM_SEGMENT;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    String[] operand_params = operand.split(" ");

    if (operand_params.length == 3) {
      query = query.replace(QueryConstants.OPERATOR, operand_params[1]);
      query = query.replace(QueryConstants.VALUE, operand_params[2]);
      query = query.replace(QueryConstants.START_DATE, startDate);
      query = query.replace(QueryConstants.END_DATE, endDate);

      return query;
    } else {
      log.error("Error while generating query for CustomerAOV for botRef: {}", botRef);
      return "";
    }
  }

  public String generateQueryForOrdersCustomSegment(Long botRef,String operand,String startDate,String endDate) {
    String query = NativeQueries.NUMBER_OF_ORDERS_CUSTOM_SEGMENT;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    String[] operand_params = operand.split(" ");

    if (operand_params.length == 3) {
      query = query.replace(QueryConstants.OPERATOR, operand_params[1]);
      query = query.replace(QueryConstants.VALUE, operand_params[2]);
      query = query.replace(QueryConstants.START_DATE, startDate);
      query = query.replace(QueryConstants.END_DATE, endDate);

      return query;
    } else {
      log.error("Error while generating query for Number of Orders for botRef: {}", botRef);
      return "";
    }
  }

  public String generateQueryForLastOrderDaysCustomSegment(Long botRef,String operand,String startDate,String endDate) {
    String query = NativeQueries.LAST_ORDER_DAYS_CUSTOM_SEGMENT;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    String[] operand_params = operand.split(" ");

    if (operand_params.length == 3) {
      query = query.replace(QueryConstants.GAP, String.valueOf(operand_params[2]));
      query = query.replace(QueryConstants.END_DATE, endDate);
      return query;
    } else {
      log.error("Error while generating query for Last Order Days for botRef: {}", botRef);
      return "";
    }
  }

  public String generateQueryForAmountSpentCustomSegment(Long botRef,String operand,String startDate,String endDate) {
    String query = NativeQueries.AMOUNT_SPENT_CUSTOM_SEGMENT;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    String[] operand_params = operand.split(" ");

    if (operand_params.length == 3) {
      query = query.replace(QueryConstants.OPERATOR, operand_params[1]);
      query = query.replace(QueryConstants.VALUE, operand_params[2]);
      query = query.replace(QueryConstants.START_DATE, startDate);
      query = query.replace(QueryConstants.END_DATE, endDate);

      return query;
    } else {
      log.error("Error while generating query for Spend for botRef: {}", botRef);
      return "";
    }
  }

  public String generateQueryForProductTypeCustomSegment(Long botRef,String operand,String startDate,String endDate,Set<String>productTypes) {
    String query = NativeQueries.GET_CUSTOMERS_FOR_PRODUCT_TYPE;
    query = query.replace(Constants.BOT_REF,botRef.toString());
    query = query.replace(QueryConstants.PRODUCT_TYPES,productTypes.toString());

    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET,QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET,QueryConstants.CLOSING_ROUND_BRACKET);

    query = query.replace(QueryConstants.START_DATE, startDate);
    query = query.replace(QueryConstants.END_DATE, endDate);

    return query;
  }

  public String generateQueryForCollectionCustomSegment(Long botRef, String operand,String startDate,String endDate, Set<String>collections) {
    String query = NativeQueries.GET_CUSTOMERS_FOR_COLLECTION;
    query = query.replace(Constants.BOT_REF,botRef.toString());
    query = query.replace(QueryConstants.COLLECTIONS,collections.toString());

    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET,QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET,QueryConstants.CLOSING_ROUND_BRACKET);

    query = query.replace(QueryConstants.START_DATE, startDate);
    query = query.replace(QueryConstants.END_DATE, endDate);

    return query;
  }

  public String generateQueryForCountryCustomSegment(Long botRef, String operand,String startDate,String endDate, Set<String>countries) {
    String query = NativeQueries.GET_CUSTOMERS_FOR_COUNTRY;
    query = query.replace(Constants.BOT_REF,botRef.toString());
    query = query.replace(QueryConstants.COUNTRIES,countries.toString());

    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET,QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET,QueryConstants.CLOSING_ROUND_BRACKET);

    query = query.replace(QueryConstants.START_DATE, startDate);
    query = query.replace(QueryConstants.END_DATE, endDate);

    return query;
  }

  public String generateQueryForCityCustomSegment(Long botRef,String operand,String startDate,String endDate,Set<String>cities) {
    String query = NativeQueries.GET_CUSTOMERS_FOR_CITIES;
    query = query.replace(Constants.BOT_REF,botRef.toString());
    query = query.replace(QueryConstants.CITIES,cities.toString());

    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET,QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET,QueryConstants.CLOSING_ROUND_BRACKET);

    query = query.replace(QueryConstants.START_DATE, startDate);
    query = query.replace(QueryConstants.END_DATE, endDate);

    return query;
  }

  @Override
  public DataAnalyticsResponse<List<String>> getProductType(Long botRef) {
    Map<String, List<String>> productTypes = new HashMap<>();
    DataAnalyticsResponse<List<String>> response = new DataAnalyticsResponse<>();
    try {
      String query = NativeQueries.PRODUCT_TYPE_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        productTypes = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<String, List<String>>>() {
        });
      }
    } catch (Exception e) {
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.info("Error while getting List of ProductTypes for: botRef:{}", botRef, e);
    }
    response.setResponseObject(productTypes.get("product_type"));
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<String>> getCollection(Long botRef) {
    Map<String, List<String>> collections = new HashMap<>();
    DataAnalyticsResponse<List<String>> response = new DataAnalyticsResponse<>();
    try {
      String query = NativeQueries.COLLECTION_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        collections = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<String, List<String>>>() {
        });
      }
    } catch (Exception e) {
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.info("Error while getting List of Collections for: botRef:{}", botRef, e);
    }
    response.setResponseObject(collections.get(QueryConstants.COLLECTIONS));
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<String>> getCountry(Long botRef) {
    Map<String, List<String>> countries = new HashMap<>();
    DataAnalyticsResponse<List<String>> response = new DataAnalyticsResponse<>();
    try {
      String query = NativeQueries.COUNTRY_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        countries = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<String, List<String>>>() {
        });
      }
    } catch (Exception e) {
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.info("Error while getting List of Collections for: botRef:{}", botRef, e);
    }
    response.setResponseObject(countries.get(QueryConstants.COUNTRIES));
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

  @Override
  public DataAnalyticsResponse<List<String>> getCity(Long botRef) {
    Map<String,List<String>> cityList = new HashMap<>();
    DataAnalyticsResponse<List<String>> response = new DataAnalyticsResponse<>();
    try {
      String query = NativeQueries.CITY_QUERY;
      query = query.replace(Constants.BOT_REF,botRef.toString());
      JSONObject requestBody = new JSONObject();
      requestBody.put(Constants.QUERY, query);
      log.debug("Request body for query to duckDB: {}", requestBody);
      Response<JsonNode> etlResponse = etlEngineRestUtility.executeQuery(requestBody).execute();
      if (Objects.nonNull(etlResponse) && etlResponse.isSuccessful() && Objects.nonNull(etlResponse.body())) {
        cityList = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<String, List<String>>>() {
        });
      }
    } catch (Exception e) {
      response.setStatus(ResponseStatusCode.PROCESSING_ERROR);
      log.info("Error while getting List of Cities for: botRef:{}", botRef, e);
    }
    response.setResponseObject(cityList.get(QueryConstants.CITIES));
    response.setStatus(ResponseStatusCode.SUCCESS);
    return response;
  }

}

