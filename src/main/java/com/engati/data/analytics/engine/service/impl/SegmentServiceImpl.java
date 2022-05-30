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
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.repository.SegmentRepository;
import com.engati.data.analytics.engine.service.CustomerSegmentationConfigurationService;
import com.engati.data.analytics.engine.service.SegmentService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import retrofit2.Response;

import java.io.File;
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
  private SegmentRepository segmentRepository;

  @Autowired
  private CommonUtils commonUtils;

  @Autowired
  private EtlEngineRestUtility etlEngineRestUtility;

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private List<CustomerSegmentationResponse> getDetailsforCustomerSegments(Set<Long> customerList, Long botRef) {
    log.info("Getting details for Customer segment with botRef : {}", botRef);
    List<CustomerSegmentationResponse> customerSegmentationResponseList = new ArrayList<>();
    List<Map<Long, Object>> customerDetails = segmentRepository.findByShopifyCustomerId(customerList);

    String storeAOV = null;
    storeAOV = getStoreAOV(botRef);
    Map<Long, Map<String, Object>> customerAOV = getCustomerAOV(customerList, botRef);
    Map<Long, Map<String, Integer>> ordersLastMonth = getOrdersForLastXMonth(customerList, botRef, 1);
    Map<Long, Map<String, Integer>> ordersLast6Months = getOrdersForLastXMonth(customerList, botRef, 6);
    Map<Long, Map<String, Integer>> ordersLast12Months = getOrdersForLastXMonth(customerList, botRef, 12);
    for (Long customerId : customerList) {
      CustomerSegmentationResponse customerSegmentationResponse = new CustomerSegmentationResponse();
      for (Map<Long, Object> row : customerDetails) {
        if ((row.get(Constants.CUSTOMER_ID)).toString().equals(customerId.toString())) {
          customerSegmentationResponse.setCustomerEmail(String.valueOf(row.get(Constants.CUSTOMER_EMAIL)));
          customerSegmentationResponse.setCustomerPhone(String.valueOf(row.get(Constants.CUSTOMER_PHONE)));
          customerSegmentationResponse.setCustomerName(String.valueOf(row.get(Constants.CUSTOMER_NAME)));
        }
        continue;
      }
      customerSegmentationResponse.setStoreAOV(Double.valueOf(storeAOV));
      try {
        customerSegmentationResponse.setCustomerAOV((Double) customerAOV.get(customerId).getOrDefault(QueryConstants.AOV, Constants.DEFAULT_AOV_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setCustomerAOV(Double.valueOf(Constants.DEFAULT_AOV_VALUE));
      }
      try {
        customerSegmentationResponse.setOrdersInLastOneMonth(ordersLastMonth.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_1_MONTH, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastOneMonth(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersInLastSixMonths(ordersLast6Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_6_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastSixMonths(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersInLastTwelveMonths(ordersLast12Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_12_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastTwelveMonths(Constants.DEFAULT_ORDER_VALUE);
      }
      customerSegmentationResponseList.add(customerSegmentationResponse);
    }
    return customerSegmentationResponseList;
  }

  @Override
  public DataAnalyticsResponse<List<CustomerSegmentationResponse>> getCustomersForSystemSegment(Long botRef, String segmentName) {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, segment: {}", botRef, segmentName);
    DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = new DataAnalyticsResponse<>();
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails = customerSegmentationConfigurationService.getSystemSegmentConfigByBotRefAndSegment(botRef, segmentName);
    log.info("Checking for Config values for the segment values for {} for botRef: {}", segmentName, botRef);
    Set<Long> resultSet = null;
    Set<Long> recencySegment = null;
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
    try {
      String fileName = getOutputFileName(botRef, segmentName);
      if (Objects.nonNull(fileName)) {
        if (!CollectionUtils.isEmpty(resultSet)) {
          List<CustomerSegmentationResponse> customerDetail = getDetailsforCustomerSegments(resultSet, botRef);
          if (!CommonUtils.createCsv(customerDetail, botRef, segmentName, fileName)) {
            response.setResponseStatusCode(ResponseStatusCode.CSV_CREATION_EXCEPTION);
          }
        } else {
          File emptyFile = new File(fileName);
          if (emptyFile.exists()) {
            emptyFile.delete();
          }
          if (!emptyFile.createNewFile()) {
            log.error("Error creating empty file for empty segment for botRef: {} for segmentName: {}", botRef, segmentName);
          }
          response.setResponseStatusCode(ResponseStatusCode.EMPTY_SEGMENT);
        }
      }
    } catch (Exception e) {
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Exception caught while getting customer details for botRef: {}, segment: {}", botRef, segmentName, e);
    }
    return response;
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
      log.error("Exception while getting StoreAOV for botRef: {}", botRef, e);
    }
    return storeAov;
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
      log.error("Error while getting Customer AOV for: botRef:{}", botRef, e);
    }
    return customerAOV;
  }

  private Map<Long, Map<String, Integer>> getOrdersForLastXMonth(Set<Long> customerIds, Long botRef, Integer Month) {
    Map<Long, Map<String, Integer>> customerOrders = new HashMap<>();
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
        customerOrders = MAPPER.readValue(MAPPER.writeValueAsString(etlResponse.body().get(Constants.RESPONSE_OBJECT)), new TypeReference<Map<Long, Map<String, Integer>>>() {
        });
      }
    } catch (Exception e) {
      log.error("Error while getting Orders For Last X Month for botRef: {}", botRef, e);
    }
    return customerOrders;
  }

  @Override
  public Map<String, Object> getCustomersForCustomSegment(Long botRef, CustomSegmentRequest customSegmentRequest) {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, customSegmentRequest: {}", botRef, customSegmentRequest);
    String segmentCondition = customSegmentRequest.getSegmentCondition();
    String segmentName = customSegmentRequest.getSegmentName();
    DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = new DataAnalyticsResponse<>();
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    Pattern segmentOperators = Pattern.compile("(?i) AND | OR ");
    Matcher OperatorMatcher = segmentOperators.matcher(segmentCondition);
    ArrayList<String> operators = new ArrayList<>();

    while (OperatorMatcher.find()) {
      operators.add(OperatorMatcher.group().trim());
    }

    if (operators.size() > 4) {
      response.setResponseObject(null);
      response.setResponseStatusCode(ResponseStatusCode.OPERATORS_PERMISSIBLE_LIMITS_REACHED);
    }

    String[] operands = segmentCondition.split("(?i) AND | OR ");
    Set<Long> small_query_parameter_set = new HashSet<Long>();
    Long small_set_execution_time = 0L;
    String query = segmentCondition;
    String query_for_operand = "";
    for (int index = 0; index < operands.length; index++) {
      String operand = operands[index];
      if (operand.contains("ORDERS")) {
        query_for_operand = generateQueryForOrdersInLastXMonthsWithFilters(operand, botRef);
      } else if (operand.contains("AOV")) {
        query_for_operand = generateQueryForCustomerAOVWithFilters(operand, botRef);
      } else {
        response.setResponseObject(null);
        response.setResponseStatusCode(ResponseStatusCode.INVALID_ATTRIBUTES_PROVIDED);
      }
      log.info("Executing following query : " + query_for_operand);
      query = query.replace(operand, query_for_operand);
      Map<String, Object> parameter_response = getCustomerListForParameter(query_for_operand);
      Set<Long> parameter_set = (Set<Long>) parameter_response.get("response");
      small_set_execution_time += (Long) parameter_response.get("execution_time");
      if (index == 0) {
        small_query_parameter_set.addAll(parameter_set);
      } else {
        switch (operators.get(index - 1)) {
          case "AND":
            small_query_parameter_set.retainAll(parameter_set);
            break;
          case "OR":
            small_query_parameter_set.addAll(parameter_set);
            break;
          default:
            response.setResponseObject(null);
            response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
        }
      }
    }
    query = query.replace("AND", "\nINTERSECT\n");
    query = query.replace("OR", "\nUNION\n");
    Map<String, Object> parameter_response = getCustomerListForParameter(query);
    Set<Long> combined_query_parameter_set = (Set<Long>) parameter_response.get("response");
    Long combined_set_execution_time = (Long) parameter_response.get("execution_time");
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("small_set", small_query_parameter_set);
    resultMap.put("small_set_execution_time", small_set_execution_time);
    resultMap.put("combined_set", combined_query_parameter_set);
    resultMap.put("combined_set_execution_time", combined_set_execution_time);
    return resultMap;
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
    query = query.replace(QueryConstants.OPERATOR, operand_params[1]);
    if (operand_params[2] == "STORE_AOV") {
      query = query.replace(QueryConstants.VALUE, getStoreAOV(botRef));
    } else {
      query = query.replace(QueryConstants.VALUE, operand_params[2]);
    }
    query = query.replace(QueryConstants.GAP, "3"); // gap is defaulted for AOV related queries to 3 months
    return query;
  }
}
