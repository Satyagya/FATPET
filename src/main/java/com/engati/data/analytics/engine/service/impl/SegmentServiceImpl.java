package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.QueryOperators;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;
import com.engati.data.analytics.engine.repository.SegmentRepository;
import com.engati.data.analytics.engine.service.SegmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.*;
import java.util.*;


@Slf4j
@Service("com.engati.data.analytics.engine.service.SegmentsService")
public class SegmentServiceImpl implements SegmentService {

  Connection conn;
  {
    try {
      conn = DriverManager.getConnection(Constants.DUCKDB_CONNECTION_URI);
    } catch (SQLException e) {
      log.error("Failed to connect to DuckDB", e);
    }
  }

  @Autowired
  private StoreSegmentationConfigurationServiceImpl storeSegmentationConfigurationService;

  @Autowired
  private SegmentRepository segmentRepository;


  private List<CustomerSegmentationResponse> getDetailsforCustomerSegments(Set<Long> customerList, Long botRef) {
    List<CustomerSegmentationResponse> finalList = new ArrayList<>();
    List<Map<Long, Object>> customerDetails = segmentRepository.findByShopifyCustomerId(customerList);

    String storeAOV = null;
    try {
      storeAOV = getStoreAOV(botRef);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    Map<Long, Map<String, Object>> customerAOV = getCustomerAOV(customerList, botRef);
    Map<Long, Map<String, Object>> ordersLastMonth = getOrdersForLastXMonth(customerList, botRef,1);
    Map<Long, Map<String, Object>> ordersLast6Months = getOrdersForLastXMonth(customerList, botRef, 6);
    Map<Long, Map<String, Object>> ordersLast12Months = getOrdersForLastXMonth(customerList, botRef, 12);
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
      customerSegmentationResponse.setCustomerAOV((Double) customerAOV.get(customerId).getOrDefault(QueryConstants.AOV, Constants.DEFAULT_AOV_VALUE));
      try {
        customerSegmentationResponse.setOrdersLast1Month((Long) ordersLastMonth.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_1_MONTH, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersLast1Month(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersLast6Month((Long) ordersLast6Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_6_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersLast6Month(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersLast12Month((Long) ordersLast12Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_12_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersLast12Month(Constants.DEFAULT_ORDER_VALUE);
      }
      finalList.add(customerSegmentationResponse);
    }
    return finalList;
  }

  @Override
  public DataAnalyticsResponse<List<CustomerSegmentationResponse>>  getCustomersForSegment(Long customerId, Long botRef, String segmentName) {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = new DataAnalyticsResponse<>();
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    DataAnalyticsResponse<SegmentationConfigurationResponse> configDetails = storeSegmentationConfigurationService.getConfigByBotRefAndSegment(customerId, botRef, segmentName);
    log.info("Checking for Config values for the segment values for {} for botRef: {}, and customerId: {}", segmentName, botRef, customerId);
    Set<Long> resultSet = null;
    Set<Long> recencySegment = null;
    if (configDetails.getResponseObject().getRecencyMetric() != null) {
      log.info("Recency configurations found for the segmentName {}", segmentName);
      recencySegment = getRecencySegment(configDetails);
    }
    Set<Long> frequencySegment = null;
    if (configDetails.getResponseObject().getFrequencyMetric() != null) {
      log.info("Frequency configurations found for the segmentName {}", segmentName);
      frequencySegment = getFrequencySegment(configDetails);
    }
    Set<Long> monetarySegment = null;
    if (configDetails.getResponseObject().getMonetaryMetric() != null) {
      log.info("Monetary configurations found for the segmentName {}", segmentName);
      monetarySegment = getMonetarySegment(configDetails);
    }
    resultSet = getIntersectionForSegments(recencySegment, frequencySegment, monetarySegment);
    try {
      if (resultSet.size() != 0) {
        List<CustomerSegmentationResponse> customerDetail = getDetailsforCustomerSegments(resultSet, botRef);
        response.setResponseObject(customerDetail);
      } else {
        response.setResponseStatusCode(ResponseStatusCode.EMPTY_SEGMENT);
      }
    } catch (Exception e) {
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Unhandled exception caught while getting customer details for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    }
    return response;
  }
  private Set<Long> getIntersectionForSegments(Set<Long> set1, Set<Long> set2, Set<Long> set3){
    List<Set<Long>> ListSegment = new ArrayList<>();
    if(!CollectionUtils.isEmpty(set1))
      ListSegment.add(set1);
    if(!CollectionUtils.isEmpty(set2))
      ListSegment.add(set2);
    if(!CollectionUtils.isEmpty(set3))
      ListSegment.add(set3);

    if (ListSegment.size() == 0)
      return null;
    else if (ListSegment.size() == 1)
      return ListSegment.get(0);
    else if (ListSegment.size() == 2){
      Set<Long> resSet1 = ListSegment.get(0);
      Set<Long> resSet2 = ListSegment.get(1);
      resSet1.retainAll(resSet2);
      return resSet1;
    }
    else {
      Set<Long> resSet1 = ListSegment.get(0);
      Set<Long> resSet2 = ListSegment.get(1);
      Set<Long> resSet3 = ListSegment.get(2);
      resSet1.retainAll(resSet2);
      resSet1.retainAll(resSet3);
      return resSet1;
    }
  }

  private Set<Long> getMonetarySegment(DataAnalyticsResponse<SegmentationConfigurationResponse> configDetails)  {
    String query = NativeQueries.MONETARY_QUERY;
    query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getMonetaryOperator()));
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(QueryConstants.METRIC, configDetails.getResponseObject().getMonetaryMetric());
    if (configDetails.getResponseObject().getMonetaryValue().equals(QueryConstants.STORE_AOV)) {
      try {
        query = query.replace(QueryConstants.VALUE, getStoreAOV(configDetails.getResponseObject().getBotRef()));
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    query = query.replace(QueryConstants.VALUE, configDetails.getResponseObject().getMonetaryValue());
    Set<Long> monetaryList = executeQuery(query);
    return monetaryList;
  }

  private Set<Long> getFrequencySegment(DataAnalyticsResponse<SegmentationConfigurationResponse> configDetails) {
    String query = NativeQueries.FREQUENCY_QUERY;
    query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getFrequencyOperator()));
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(QueryConstants.GAP, configDetails.getResponseObject().getFrequencyValue().toString());
    query = query.replace(QueryConstants.ORDERS_CONFIGURED, configDetails.getResponseObject().getFrequencyMetric().toString());
    Set<Long> frequencyList = executeQuery(query);
    return frequencyList;
  }

  private Set<Long> getRecencySegment(DataAnalyticsResponse<SegmentationConfigurationResponse> configDetails)  {
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
    Set<Long> recencyList = executeQuery(query);
    return recencyList;
  }

  private Map<Long, Map<String, Object>> executeQueryForDetails(String query) {
    Statement statement = null;
    Map<Long, Map<String, Object>> querySet = new HashMap<>();
    try {
      statement = conn.createStatement();
      ResultSet rs = statement.executeQuery(query);
      ResultSetMetaData md = rs.getMetaData();
      int columns = md.getColumnCount();

      while (rs.next()) {
        HashMap row = new HashMap(columns);
        for (int i = 1; i <= columns; ++i) {
          row.put(md.getColumnName(i), rs.getObject(i));
        }
        querySet.put((Long) row.get(Constants.CUSTOMER_ID), row);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return querySet;
  }

  private Set<Long> executeQuery(String query) {
    Statement statement = null;
    Set<Long> querySet = new LinkedHashSet<>();
    try {
      statement = conn.createStatement();
      ResultSet rs = statement.executeQuery(query);
      ResultSetMetaData md = rs.getMetaData();
      int columns = md.getColumnCount();
      while (rs.next()) {
        HashMap row = new HashMap(columns);
        for (int i = 1; i <= columns; ++i) {
          row.put(md.getColumnName(i), rs.getObject(i));
        }
        querySet.add((Long) row.get(Constants.CUSTOMER_ID));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return querySet;
  }

  private String getStoreAOV(Long botRef) throws SQLException {
    Statement statement = null;
    ResultSet rs = null;
    try {
      statement = conn.createStatement();
      String query = NativeQueries.STORE_AOV_QUERY;
      String result = query.replace(Constants.BOT_REF, botRef.toString());
      rs = statement.executeQuery(result);
//    Todo change the var rs
      rs.next();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return String.valueOf(rs.getDouble(QueryConstants.STORE_AOV));
  }

  private Map<Long, Map<String, Object>> getCustomerAOV(Set<Long> customerIds, Long botRef) {
    String query = NativeQueries.CUSTOMER_AOV_QUERY;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
    Map<Long, Map<String, Object>> customerAOV = executeQueryForDetails(query);
    return customerAOV;
  }

  private Map<Long, Map<String, Object>> getOrdersForLastXMonth(Set<Long> customerIds, Long botRef, Integer Month){
    String query = NativeQueries.ORDERS_FOR_X_MONTHS;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(QueryConstants.GAP, Month.toString());
    query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
    if (Month == 1) query = query.replace(QueryConstants.MONTHS, QueryConstants.MONTH);
    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
    Map<Long, Map<String, Object>> customerOrders = executeQueryForDetails(query);
    return customerOrders;
  }

}
