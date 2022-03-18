package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.common.model.CustomerSegmentResponse;
import com.engati.data.analytics.engine.common.model.SegmentConfigResponse;
import com.engati.data.analytics.engine.constants.Constants;
import com.engati.data.analytics.engine.constants.QueryConstants;
import com.engati.data.analytics.engine.constants.ResponseStatusCode;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;
import com.engati.data.analytics.engine.repository.GenerateSegmentsRepository;
import com.engati.data.analytics.engine.service.GenerateSegmentsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;


@Slf4j
@Service("com.engati.data.analytics.engine.service.GenerateSegmentsService")
public class GenerateSegmentsServiceImpl implements GenerateSegmentsService {

  Connection conn;
  {
    try {
      conn = DriverManager.getConnection(Constants.DUCKDB_CONNECTION_URI);
    } catch (SQLException e) {
      e.printStackTrace();
      log.error("Failed to connect to DuckDB");
    }
  }

  @Autowired
  private StoreSegmentationConfigurationServiceImpl storeSegmentationConfigurationService;

  @Autowired
  private GenerateSegmentsRepository generateSegmentsRepository;


  public List<CustomerSegmentationResponse> getDetailsforCustomerSegments(Set<Long> customerList, Long botRef) throws SQLException {
    List<CustomerSegmentationResponse> finalList = new ArrayList<>();
    List<Map<Long, Object>> customerDetails = generateSegmentsRepository.findByShopifyCustomerId(customerList);

    String storeAOV = getStoreAOV(botRef);
    Map<Long, Map<String, Object>> customerAOV = getCustomerAOV(customerList, botRef);
    Map<Long, Map<String, Object>> ordersLastMonth = getOrdersForLastXMonth(customerList, botRef, Constants.ONE);
    Map<Long, Map<String, Object>> ordersLast6Months = getOrdersForLastXMonth(customerList, botRef, Constants.SIX);
    Map<Long, Map<String, Object>> ordersLast12Months = getOrdersForLastXMonth(customerList, botRef, Constants.TWELVE);
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
      customerSegmentationResponse.setCustomerAOV((Double) customerAOV.get(customerId).getOrDefault(Constants.AOV, Constants.DEFAULT_AOV_VALUE));
      try {
        customerSegmentationResponse.setOrdersLast1Month((Long) ordersLastMonth.get(customerId).getOrDefault(Constants.ORDERS_LAST_1_MONTH, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersLast1Month(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersLast6Month((Long) ordersLast6Months.get(customerId).getOrDefault(Constants.ORDERS_LAST_6_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersLast6Month(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersLast12Month((Long) ordersLast12Months.get(customerId).getOrDefault(Constants.ORDERS_LAST_12_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersLast12Month(Constants.DEFAULT_ORDER_VALUE);
      }
      finalList.add(customerSegmentationResponse);
    }
    return finalList;
  }

  @Override
  public CustomerSegmentResponse<List<CustomerSegmentationResponse>> getCustomersForSegment(Long customerId, Long botRef, String segmentName) throws SQLException {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    CustomerSegmentResponse<List<CustomerSegmentationResponse>> response = new CustomerSegmentResponse<>();
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    SegmentConfigResponse<SegmentationConfigurationResponse> configDetails = storeSegmentationConfigurationService.getConfigByBotRefAndSegment(customerId, botRef, segmentName);
    log.info("Checking for Config values for the segment values for {} for botRef: {}, and customerId: {}", segmentName, botRef, customerId);
    Set<Long> resultSet = new HashSet<>();
    Set<Long> recencySegment = null;
    if (configDetails.getResponseObject().getRecencyMetric() != null) {
      log.info("Recency configurations found for the segmentName {}", segmentName);
      recencySegment = getRecencySegment(configDetails);
      resultSet = getMergedResultForSegmentation(recencySegment, resultSet);
    }
    Set<Long> frequencySegment = null;
    if (configDetails.getResponseObject().getFrequencyMetric() != null) {
      log.info("Frequency configurations found for the segmentName {}", segmentName);
      frequencySegment = getFrequencySegment(configDetails);
      resultSet = getMergedResultForSegmentation(frequencySegment, resultSet);
    }
    Set<Long> monetarySegment = null;
    if (configDetails.getResponseObject().getMonetaryMetric() != null) {
      log.info("Monetary configurations found for the segmentName {}", segmentName);
      monetarySegment = getMonetarySegment(configDetails);
      resultSet = getMergedResultForSegmentation(monetarySegment, resultSet);
    }
    if ((configDetails.getResponseObject().getMonetaryMetric() != null) && (configDetails.getResponseObject().getRecencyMetric() != null) && (configDetails.getResponseObject().getFrequencyMetric() != null)){
      recencySegment.retainAll(frequencySegment);
      recencySegment.retainAll(monetarySegment);
      resultSet = recencySegment;
    }
    try {
      if (resultSet.size() != 0) {
        List<CustomerSegmentationResponse> customerDetail = getDetailsforCustomerSegments(resultSet, botRef);
        response.setResponseObject(customerDetail);
      }else{
        response.setResponseStatusCode(ResponseStatusCode.EMPTY_SEGMENT);
      }
    }catch (Exception e){
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Unhandled exception caught while getting customer details for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    }
    return response;
  }

  private Set<Long> getMergedResultForSegmentation(Set<Long> baseResults, Set<Long> newResults) {
    if (newResults.isEmpty()) {
      return baseResults;
    } else {
      Set<Long> set = new HashSet<>();
      for (Long longValue : baseResults) {
        if (newResults.contains(longValue)) {
          set.add(longValue);
        }
      }
      return set;
    }
  }

  private Set<Long> getMonetarySegment(SegmentConfigResponse<SegmentationConfigurationResponse> configDetails) throws SQLException {
    String query = QueryConstants.MONETARY_QUERY;
    query = query.replace(Constants.OPERATOR, getOperator(configDetails.getResponseObject().getMonetaryOperator()));
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(Constants.METRIC, configDetails.getResponseObject().getMonetaryMetric());
    if (configDetails.getResponseObject().getMonetaryValue().equals(Constants.STORE_AOV)) {
      query = query.replace(Constants.VALUE, getStoreAOV(configDetails.getResponseObject().getBotRef()));
    }
    query = query.replace(Constants.VALUE, configDetails.getResponseObject().getMonetaryValue());
    Set<Long> monetaryList = executeQuery(query);
    return monetaryList;
  }

  private Set<Long> getFrequencySegment(SegmentConfigResponse<SegmentationConfigurationResponse> configDetails) throws SQLException {
    String query = QueryConstants.FREQUENCY_QUERY;
    query = query.replace(Constants.OPERATOR, getOperator(configDetails.getResponseObject().getFrequencyOperator()));
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(Constants.GAP, configDetails.getResponseObject().getFrequencyValue().toString());
    query = query.replace(Constants.ORDERS_CONFIGURED, configDetails.getResponseObject().getFrequencyMetric().toString());
    Set<Long> frequencyList = executeQuery(query);
    return frequencyList;
  }

  private Set<Long> getRecencySegment(SegmentConfigResponse<SegmentationConfigurationResponse> configDetails) throws SQLException {
    String query = QueryConstants.RECENCY_QUERY;
    query = query.replace(Constants.OPERATOR, getOperator(configDetails.getResponseObject().getRecencyOperator()));
    if (configDetails.getResponseObject().getRecencyMetric().equals(Constants.LAST_ORDER_DATE)) {
      query = query.replace(Constants.AGGREGATOR, Constants.MAX);
    } else {
      query = query.replace(Constants.AGGREGATOR, Constants.MIN);
    }
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(Constants.GAP, configDetails.getResponseObject().getRecencyValue().toString());
    query = query.replace(Constants.COLUMN_NAME, configDetails.getResponseObject().getRecencyMetric().toLowerCase(Locale.ROOT));
    Set<Long> recencyList = executeQuery(query);
    return recencyList;
  }

  private Map<Long, Map<String, Object>> executeQueryForDetails(String query) throws SQLException {
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(query);
    ResultSetMetaData md = rs.getMetaData();
    int columns = md.getColumnCount();
    Map<Long, Map<String, Object>> querySet = new HashMap<>();
    while (rs.next()) {
      HashMap row = new HashMap(columns);
      for (int i = 1; i <= columns; ++i) {
        row.put(md.getColumnName(i), rs.getObject(i));
      }
      querySet.put((Long) row.get(Constants.CUSTOMER_ID), row);
    }
    return querySet;
  }

  private Set<Long> executeQuery(String query) throws SQLException {
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(query);
    ResultSetMetaData md = rs.getMetaData();
    int columns = md.getColumnCount();
    Set<Long> querySet = new LinkedHashSet<>();
    while (rs.next()) {
      HashMap row = new HashMap(columns);
      for (int i = 1; i <= columns; ++i) {
        row.put(md.getColumnName(i), rs.getObject(i));
      }
      querySet.add((Long) row.get(Constants.CUSTOMER_ID));
    }
    return querySet;
  }

  private String getStoreAOV(Long botRef) throws SQLException {
    Statement statement = conn.createStatement();
    String query = QueryConstants.STORE_AOV_QUERY;
    String result = query.replace(Constants.BOT_REF, botRef.toString());
    ResultSet rs = statement.executeQuery(result);
    rs.next();
    return String.valueOf(rs.getDouble(Constants.STORE_AOV));
  }

  private Map<Long, Map<String, Object>> getCustomerAOV(Set<Long> customerIds, Long botRef) throws SQLException {
    String query = QueryConstants.CUSTOMER_AOV_QUERY;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(Constants.CUSTOMER_SET, customerIds.toString());
    query = query.replace(Constants.OPENING_SQUARE_BRACKET, Constants.OPENING_ROUND_BRACKET);
    query = query.replace(Constants.CLOSING_SQUARE_BRACKET, Constants.CLOSING_ROUND_BRACKET);
    Map<Long, Map<String, Object>> customerAOV = executeQueryForDetails(query);
    return customerAOV;
  }

  private Map<Long, Map<String, Object>> getOrdersForLastXMonth(Set<Long> customerIds, Long botRef, Integer Month) throws SQLException {
    String query = QueryConstants.ORDERS_FOR_X_MONTHS;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(Constants.GAP, Month.toString());
    query = query.replace(Constants.CUSTOMER_SET, customerIds.toString());
    if (Month == 1) query = query.replace(Constants.MONTHS, Constants.MONTH);
    query = query.replace(Constants.OPENING_SQUARE_BRACKET, Constants.OPENING_ROUND_BRACKET);
    query = query.replace(Constants.CLOSING_SQUARE_BRACKET, Constants.CLOSING_ROUND_BRACKET);
    Map<Long, Map<String, Object>> customerOrders = executeQueryForDetails(query);
    return customerOrders;
  }

  private String getOperator(String Operator) {
    if (Operator.equals(Constants.LTE)) return Constants.LTE_OPERATOR;
    else if (Operator.equals(Constants.LT)) return Constants.LT_OPERATOR;
    else if (Operator.equals(Constants.GTE)) return Constants.GTE_OPERATOR;
    else if (Operator.equals(Constants.GT)) return Constants.GT_OPERATOR;
    else return Constants.EQUAL_OPERATOR;
  }
}
