package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.Utils.CommonUtils;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.NativeQueries;
import com.engati.data.analytics.engine.constants.constant.QueryConstants;
import com.engati.data.analytics.engine.constants.enums.QueryOperators;
import com.engati.data.analytics.engine.constants.enums.ResponseStatusCode;
import com.engati.data.analytics.engine.entity.ShopifyCustomer;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationConfigurationResponse;
import com.engati.data.analytics.engine.repository.SegmentRepository;
import com.engati.data.analytics.engine.service.SegmentService;
import com.engati.data.analytics.engine.service.CustomerSegmentationConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.*;
import java.util.*;


@Slf4j
@Service("com.engati.data.analytics.engine.service.SegmentsService")
public class SegmentServiceImpl implements SegmentService {

  @Autowired
  private CustomerSegmentationConfigurationService customerSegmentationConfigurationService;

  @Autowired
  private SegmentRepository segmentRepository;

  @Autowired
  private CommonUtils commonUtils;

  private List<CustomerSegmentationResponse> getDetailsforCustomerSegments(Set<Long> customerList, Long botRef) {
    List<CustomerSegmentationResponse> customerSegmentationResponseList = new ArrayList<>();
    List<Map<Long, Object>> customerDetails = segmentRepository.findByShopifyCustomerId(customerList);

    String storeAOV = null;
    try {
      storeAOV = getStoreAOV(botRef);
    } catch (SQLException e) {
      log.error("Exception while getting StoreAOV for botRef: {}", botRef, e);
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
        customerSegmentationResponse.setOrdersInLastOneMonth((Long) ordersLastMonth.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_1_MONTH, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastOneMonth(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersInLastSixMonths((Long) ordersLast6Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_6_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastSixMonths(Constants.DEFAULT_ORDER_VALUE);
      }
      try {
        customerSegmentationResponse.setOrdersInLastTwelveMonths((Long) ordersLast12Months.get(customerId).getOrDefault(QueryConstants.ORDERS_LAST_12_MONTHS, Constants.DEFAULT_ORDER_VALUE));
      } catch (NullPointerException e) {
        customerSegmentationResponse.setOrdersInLastTwelveMonths(Constants.DEFAULT_ORDER_VALUE);
      }
      customerSegmentationResponseList.add(customerSegmentationResponse);
    }
    return customerSegmentationResponseList;
  }

  @Override
  public DataAnalyticsResponse<List<CustomerSegmentationResponse>>  getCustomersForSegment(Long customerId, Long botRef, String segmentName) {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
    DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = new DataAnalyticsResponse<>();
    response.setResponseStatusCode(ResponseStatusCode.SUCCESS);
    DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails = customerSegmentationConfigurationService.getConfigByBotRefAndSegment(customerId, botRef, segmentName);
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
      if (!CollectionUtils.isEmpty(resultSet)) {
        List<CustomerSegmentationResponse> customerDetail = getDetailsforCustomerSegments(resultSet, botRef);
        response.setResponseObject(customerDetail);
      } else {
        response.setResponseStatusCode(ResponseStatusCode.EMPTY_SEGMENT);
      }
    } catch (Exception e) {
      response.setResponseStatusCode(ResponseStatusCode.PROCESSING_ERROR);
      log.error("Exception caught while getting customer details for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName, e);
    }
    return response;
  }
  private Set<Long> getIntersectionForSegments(Set<Long> recencySet, Set<Long> frequencySet, Set<Long> monetarySet){
    List<Set<Long>> listSegment = new ArrayList<>();
    if(!CollectionUtils.isEmpty(recencySet))
      listSegment.add(recencySet);
    if(!CollectionUtils.isEmpty(frequencySet))
      listSegment.add(frequencySet);
    if(!CollectionUtils.isEmpty(monetarySet))
      listSegment.add(monetarySet);

    if (listSegment.size() == 0)
      return null;
    else if (listSegment.size() == 1)
      return listSegment.get(0);
    else if (listSegment.size() == 2){
      Set<Long> resSet1 = listSegment.get(0);
      Set<Long> resSet2 = listSegment.get(1);
      resSet1.retainAll(resSet2);
      return resSet1;
    }
    else {
      Set<Long> resSet1 = listSegment.get(0);
      Set<Long> resSet2 = listSegment.get(1);
      Set<Long> resSet3 = listSegment.get(2);
      resSet1.retainAll(resSet2);
      resSet1.retainAll(resSet3);
      return resSet1;
    }
  }

  private Set<Long> getMonetarySegment(DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails)  {
    String query = NativeQueries.MONETARY_QUERY;
    query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getMonetaryOperator()));
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(QueryConstants.METRIC, configDetails.getResponseObject().getMonetaryMetric());
    if (configDetails.getResponseObject().getMonetaryValue().equals(QueryConstants.STORE_AOV)) {
      try {
        query = query.replace(QueryConstants.VALUE, getStoreAOV(configDetails.getResponseObject().getBotRef()));
      } catch (SQLException e) {
        log.error("Exception while getting StoreAOV for botRef: {}", configDetails.getResponseObject().getBotRef().toString(), e);
      }
    }
    query = query.replace(QueryConstants.VALUE, configDetails.getResponseObject().getMonetaryValue());
    Set<Long> monetaryList = commonUtils.executeQuery(query);
    return monetaryList;
  }

  private Set<Long> getFrequencySegment(DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails) {
    String query = NativeQueries.FREQUENCY_QUERY;
    query = query.replace(QueryConstants.OPERATOR, QueryOperators.getOperator(configDetails.getResponseObject().getFrequencyOperator()));
    query = query.replace(Constants.BOT_REF, configDetails.getResponseObject().getBotRef().toString());
    query = query.replace(QueryConstants.GAP, configDetails.getResponseObject().getFrequencyValue().toString());
    query = query.replace(QueryConstants.ORDERS_CONFIGURED, configDetails.getResponseObject().getFrequencyMetric().toString());
    Set<Long> frequencyList = commonUtils.executeQuery(query);
    return frequencyList;
  }

  private Set<Long> getRecencySegment(DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> configDetails)  {
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
    Set<Long> recencyList = commonUtils.executeQuery(query);
    return recencyList;
  }



  private String getStoreAOV(Long botRef) throws SQLException {
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = commonUtils.connection.createStatement();
      String query = NativeQueries.STORE_AOV_QUERY;
      query = query.replace(Constants.BOT_REF, botRef.toString());
      log.info(query);
      resultSet = statement.executeQuery(query);
      resultSet.next();
    } catch (SQLException e) {
     log.error("Exception while getting StoreAOV for botRef: {}", botRef, e);
    }
    return String.valueOf(resultSet.getDouble(QueryConstants.STORE_AOV));
  }

  private Map<Long, Map<String, Object>> getCustomerAOV(Set<Long> customerIds, Long botRef) {
    String query = NativeQueries.CUSTOMER_AOV_QUERY;
    query = query.replace(Constants.BOT_REF, botRef.toString());
    query = query.replace(QueryConstants.CUSTOMER_SET, customerIds.toString());
    query = query.replace(QueryConstants.OPENING_SQUARE_BRACKET, QueryConstants.OPENING_ROUND_BRACKET);
    query = query.replace(QueryConstants.CLOSING_SQUARE_BRACKET, QueryConstants.CLOSING_ROUND_BRACKET);
    Map<Long, Map<String, Object>> customerAOV = commonUtils.executeQueryForDetails(query, Constants.CUSTOMER_ID );
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
    Map<Long, Map<String, Object>> customerOrders = commonUtils.executeQueryForDetails(query, Constants.CUSTOMER_ID );
    return customerOrders;
  }

}
