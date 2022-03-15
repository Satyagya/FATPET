package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.common.model.SegmentConfigResponse;
import com.engati.data.analytics.engine.constants.Constants;
import com.engati.data.analytics.engine.constants.QueryConstants;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;
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
      conn = DriverManager.getConnection("jdbc:duckdb:");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Autowired
  private StoreSegmentationConfigurationServiceImpl storeSegmentationConfigurationService;


  @Override
  public Set<Long> getCustomersForSegment(Long customerId, Long botRef, String segmentName) throws SQLException {
    log.info("Entered getQueryForCustomerSegment while getting config for botRef: {}, customerId: {}, segment: {}", botRef, customerId, segmentName);
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
      frequencySegment= getFrequencySegment(configDetails);
      resultSet = getMergedResultForSegmentation(frequencySegment, resultSet);
    }
    Set<Long> monetarySegment = null;
    if (configDetails.getResponseObject().getMonetaryMetric() != null) {
      log.info("Monetary configurations found for the segmentName {}", segmentName);
      monetarySegment = getMonetarySegment(configDetails);
      resultSet = getMergedResultForSegmentation(monetarySegment, resultSet);
    }
    return resultSet;
  }

  private Set<Long> getMergedResultForSegmentation(Set<Long> baseResults, Set<Long> newResults){
    if (newResults.isEmpty()){
      return baseResults;
    }
    else{
    Set<Long> set = new HashSet<>();
    for (Long longValue : baseResults) {
      if (newResults.contains(longValue)){
        set.add(longValue);
      }
    }
    return set;
  }
}

  private Set<Long> getMonetarySegment(SegmentConfigResponse<SegmentationConfigurationResponse> configDetails) throws SQLException {
    String query = QueryConstants.monetaryQuery;
    query = query.replace("operator", getOperator(configDetails.getResponseObject().getMonetaryOperator()));
    query = query.replace("botRef", configDetails.getResponseObject().getBotRef().toString());
    query = query.replace("metric", configDetails.getResponseObject().getMonetaryMetric());
    if (configDetails.getResponseObject().getMonetaryValue().equals(Constants.Store_AOV)) {
      query = query.replace("value", getStoreAOV(configDetails.getResponseObject().getBotRef()));
    }
    query = query.replace("value", configDetails.getResponseObject().getMonetaryMetric());
    Set<Long> monetaryList = executeQuery(query);
    return monetaryList;
  }

  private Set<Long> getFrequencySegment(SegmentConfigResponse<SegmentationConfigurationResponse> configDetails) throws SQLException {
    String query = QueryConstants.frequencyQuery;
    query = query.replace("operator", getOperator(configDetails.getResponseObject().getFrequencyOperator()));
    query = query.replace("botRef", configDetails.getResponseObject().getBotRef().toString());
    query = query.replace("gap", configDetails.getResponseObject().getFrequencyValue().toString());
    query = query.replace("orders_configured", configDetails.getResponseObject().getFrequencyMetric().toString());
    Set<Long> frequencyList = executeQuery(query);
    return frequencyList;
  }

  private  Set<Long> getRecencySegment(SegmentConfigResponse<SegmentationConfigurationResponse> configDetails) throws SQLException {
    String query = QueryConstants.recencyQuery;
    query = query.replace("operator", getOperator(configDetails.getResponseObject().getRecencyOperator()));
    if (configDetails.getResponseObject().getRecencyMetric() == Constants.lastOrderDate) {
      query = query.replace("aggregator", Constants.max);
    } else {
      query = query.replace("aggregator", Constants.min);
    }
    query = query.replace("botRef", configDetails.getResponseObject().getBotRef().toString());
    query = query.replace("gap", configDetails.getResponseObject().getRecencyValue().toString());
    query = query.replace("col_name", configDetails.getResponseObject().getRecencyMetric().toLowerCase(Locale.ROOT));
    Set<Long> recencyList = executeQuery(query);
    return recencyList;
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
      querySet.add((Long)row.get("customer_id"));
    }
    return querySet;
  }

  private String getStoreAOV(Long botRef) throws SQLException {
    Statement statement = conn.createStatement();
    String query = QueryConstants.storeAOVQuery;
    String result = query.replace("botRef", botRef.toString());
    ResultSet rs = statement.executeQuery(result);
    rs.next();
    return String.valueOf(rs.getDouble("Store_AOV"));
  }

  private String getOperator(String Operator) {
    if (Operator.equals("LTE")) return "<=";
    else if (Operator.equals("LT")) return "<";
    else if (Operator.equals("GTE")) return ">=";
    else if (Operator.equals("GT")) return ">";
    else return "=";
  }
}
