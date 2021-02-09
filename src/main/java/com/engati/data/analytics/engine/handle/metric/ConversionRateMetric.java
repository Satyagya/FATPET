package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.response.GroupByResponse;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

@Slf4j
@Component
public class ConversionRateMetric extends MetricHandler {

  private static final String METRIC_HANDLER_NAME = "conversion_rate";

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Override
  public String getMetricName() {
    return METRIC_HANDLER_NAME;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    List<QueryResponse> responses = new ArrayList<>();
    MultiQueryMetaInfo multiQueryMetaInfo = ((MultiQueryMetaInfo) druidQueryMetaInfo);
    for (DruidQueryMetaInfo druidQuery: multiQueryMetaInfo.getMultiMetricQuery()) {
      QueryResponse response = new QueryResponse();
      responses.add(queryHandlerFactory.getQueryHandler(druidQueryMetaInfo.getQueryType().name())
          .generateAndExecuteQuery(botRef, customerId, druidQuery, response));
    }
    return getConversionRate(responses, prevResponse, multiQueryMetaInfo.getMetricName(),
        multiQueryMetaInfo.getModifier(), multiQueryMetaInfo.getLimit(),
        multiQueryMetaInfo.getDimension(), botRef, customerId);
  }

  private QueryResponse getConversionRate(List<QueryResponse> responses, QueryResponse
      prevResponse, String metric, String modifier, Integer limit, String dimension,
      Integer botRef, Integer customerId) {
    SimpleResponse conversionRateResponse = new SimpleResponse();
    if (CollectionUtils.isNotEmpty(responses)) {
      if (ResponseType.GROUP_BY.equals(responses.get(0).getType())) {
        List<GroupByResponse> groupByResponses = responses.stream()
            .map(GroupByResponse.class::cast).collect(Collectors.toList());
        conversionRateResponse.setQueryResponse(getConversionRateForGroupBy(
            groupByResponses, metric, dimension, modifier, limit, botRef, customerId));
      } else {
        List<SimpleResponse> simpleResponses = responses.stream()
            .map(SimpleResponse.class::cast).collect(Collectors.toList());
        conversionRateResponse.setQueryResponse(getConversionRateFromTimeSeries(
            simpleResponses, metric, botRef, customerId));
        conversionRateResponse = mergePreviousResponse(conversionRateResponse,
            (SimpleResponse) prevResponse);
      }
    }
    conversionRateResponse.setType(ResponseType.SIMPLE);
    return conversionRateResponse;
  }

  private List<List<Map<String, Object>>> getConversionRateFromTimeSeries(List<SimpleResponse>
      responses, String metric, Integer botRef, Integer customerId) {
    List<List<Map<String, Object>>> timeStampConversionRate = new ArrayList<>();
    try {
      SimpleResponse orderResponse = responses.stream().filter(response -> response
          .getQueryResponse().get(0).get(0).containsKey(Constants.ORDER_COUNT))
          .findFirst().get();
      SimpleResponse sessionResponse = responses.stream().filter(response -> response
          .getQueryResponse().get(0).get(0).containsKey(Constants.SESSION_COUNT))
          .findFirst().get();
      List<Map<String, Object>> conversionRate = new ArrayList<>();
      for (int timeStampIndex = 0; timeStampIndex < orderResponse.getQueryResponse().size();
          timeStampIndex++) {
        Map<String, Object> value = new HashMap<>();
        for (int resultIndex = 0; resultIndex < orderResponse.getQueryResponse()
            .get(timeStampIndex).size(); resultIndex++) {
          Integer orderCount = (Integer) orderResponse.getQueryResponse()
              .get(timeStampIndex).get(resultIndex).get(Constants.ORDER_COUNT);
          Integer sessionCount = (Integer) sessionResponse.getQueryResponse()
              .get(timeStampIndex).get(resultIndex).get(Constants.SESSION_COUNT);
          Double cvr = calculateConversionRate(orderCount, sessionCount);
          value.put(metric, Constants.DECIMAL_FORMAT.format(cvr));
          conversionRate.add(value);
        }
        timeStampConversionRate.add(conversionRate);
      }
    } catch (Exception ex) {
      log.error("ConversionRateMetric: Exception while calculating CVR from time-series query "
          + "for botRef: {}, customerId: {}", botRef, customerId, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return timeStampConversionRate;
  }

  private List<List<Map<String, Object>>> getConversionRateForGroupBy(List<GroupByResponse>
      responses, String metric, String dimensionKey, String modifier, Integer limit,
      Integer botRef, Integer customerId) {
    List<Map<String, Object>> geoCVRList = new ArrayList<>();
    try {
      GroupByResponse orderResponse = responses.stream().filter(
          response -> response.getGroupByResponse().get(0).containsKey(Constants.ORDER_COUNT)).findFirst().get();
      GroupByResponse sessionResponse = responses.stream().filter(
          response -> response.getGroupByResponse().get(0).containsKey(Constants.SESSION_COUNT)).findFirst().get();

      Map<String, Integer> geoOrderCount =
          getGeoAndMetricMap(orderResponse.getGroupByResponse(), Constants.ORDER_COUNT, dimensionKey);
      Map<String, Integer> geoSessionCount =
          getGeoAndMetricMap(sessionResponse.getGroupByResponse(), Constants.SESSION_COUNT,
              dimensionKey);
      Set<String> geoSet = geoOrderCount.keySet();
      geoSet.retainAll(geoSessionCount.keySet());
      Map<String, Double> geoByValue = new HashMap<>();
      for (String geoValue : geoSet) {
        Double cvr = calculateConversionRate(geoOrderCount.get(geoValue), geoSessionCount.get(geoValue));
        geoByValue.put(geoValue, cvr);
      }
      geoByValue = applySortAndLimit(geoByValue, modifier, limit);
      for (String geoValue : geoByValue.keySet()) {
        Map<String, Object> value = new HashMap<>();
        value.put(dimensionKey, geoValue);
        Double cvr = calculateConversionRate(geoOrderCount.get(geoValue), geoSessionCount.get(geoValue));
        value.put(metric, Constants.DECIMAL_FORMAT.format(cvr));
        geoCVRList.add(value);
      }
    } catch (Exception ex) {
      log.error("ConversionRateMetric: Exception while calculating CVR from groupBy-series query "
          + "for botRef: {}, customerId: {}", botRef, customerId, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return Collections.singletonList(geoCVRList);
  }

  private Map<String, Integer> getGeoAndMetricMap(List<Map<String, Object>> eventList,
      String metric, String geo) {
    return eventList.stream().collect(Collectors
        .toMap(key -> (String) key.get(geo), value -> (Integer) value.get(metric)));
  }

  private Map<String, Double> applySortAndLimit(Map<String, Double> geoByValue, String modifier,
      Integer limit) {
    if (Constants.TOP.equals(modifier)) {
      return geoByValue.entrySet().stream().sorted(Collections.reverseOrder(comparingByValue()))
          .limit(limit).collect(Collectors
              .toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    } else {
      return geoByValue.entrySet().stream().sorted(comparingByValue())
          .limit(limit).collect(Collectors
              .toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }
  }

  private Double calculateConversionRate(int orders, int sessions) {
    if (orders > 0 && sessions > 0) {
      return ((double) orders / (double) sessions) * 100;
    }
    return 0D;
  }

  private SimpleResponse mergePreviousResponse(SimpleResponse response,
      SimpleResponse prevResponse) {
    if (Objects.isNull(prevResponse) || Objects.isNull(prevResponse.getQueryResponse())
        || prevResponse.getQueryResponse().isEmpty()) {
      return response;
    } else {
      for (int resultIndex = 0; resultIndex < response.getQueryResponse().size(); resultIndex++) {
        for (int index = 0; index < response.getQueryResponse().get(resultIndex).size(); index++) {
          prevResponse.getQueryResponse().get(resultIndex).get(index)
              .putAll(response.getQueryResponse().get(resultIndex).get(index));
        }
      }
    }
    return prevResponse;
  }
}
